// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/experr"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/hosttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/storagetest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
)

func createTestCompressedPersistentQueue(tb testing.TB, ext storage.Extension, sizerType request.SizerType, capacity int64, compression StorageCompression) *persistentQueue[intRequest] {
	set := newSettingsWithStorage(sizerType, capacity)
	set.StorageCompression = compression
	pq := newPersistentQueue[intRequest](set)
	require.NoError(tb, pq.Start(context.Background(), hosttest.NewHost(map[component.ID]component.Component{{}: ext})))
	return pq.(*persistentQueue[intRequest])
}

func storedItem(t *testing.T, pq *persistentQueue[intRequest], index uint64) []byte {
	value, err := pq.client.Get(context.Background(), getItemKey(index))
	require.NoError(t, err)
	return value
}

func TestStorageCompression_Validate(t *testing.T) {
	for _, valid := range []StorageCompression{"", StorageCompressionNone, StorageCompressionZstd} {
		require.NoError(t, valid.Validate())
	}
	require.EqualError(t, StorageCompression("gzip").Validate(), `unsupported storage compression "gzip"`)
	assert.False(t, StorageCompression("").Enabled())
	assert.False(t, StorageCompressionNone.Enabled())
	assert.True(t, StorageCompressionZstd.Enabled())
}

func TestStorageCompression_RoundTrip(t *testing.T) {
	encoded := bytes.Repeat([]byte("otlp payload "), 1000)
	stored := compressStoredItem(StorageCompressionZstd, encoded)
	assert.True(t, bytes.HasPrefix(stored, zstdFrameMagic))
	assert.Less(t, len(stored), len(encoded)/10)
	assert.Equal(t, encoded, decompressStoredItem(stored))

	assert.Equal(t, encoded, compressStoredItem(StorageCompressionNone, encoded))
	assert.Equal(t, encoded, decompressStoredItem(encoded))

	// Bytes that only open with the magic are returned as stored, for the encoding to judge.
	notAFrame := append(append([]byte{}, zstdFrameMagic...), 1, 2, 3)
	assert.Equal(t, notAFrame, decompressStoredItem(notAFrame))
}

func TestPersistentQueue_CompressedItemsAreStoredCompressed(t *testing.T) {
	ext := storagetest.NewMockStorageExtension(nil)
	pq := createTestCompressedPersistentQueue(t, ext, request.SizerTypeItems, 1000, StorageCompressionZstd)
	require.NoError(t, pq.Offer(context.Background(), intRequest(42)))
	assert.True(t, bytes.HasPrefix(storedItem(t, pq, 0), zstdFrameMagic))
	assert.Equal(t, int64(42), pq.Size())

	require.True(t, consume(pq, func(_ context.Context, got intRequest) error {
		assert.Equal(t, intRequest(42), got)
		return nil
	}))
	assert.Equal(t, int64(0), pq.Size())
	require.NoError(t, pq.Shutdown(context.Background()))
}

func TestPersistentQueue_CompressedBytesSizerCountsStoredBytes(t *testing.T) {
	ext := storagetest.NewMockStorageExtension(nil)
	pq := createTestCompressedPersistentQueue(t, ext, request.SizerTypeBytes, 1000, StorageCompressionZstd)
	require.NoError(t, pq.Offer(context.Background(), intRequest(7)))
	require.NoError(t, pq.Offer(context.Background(), intRequest(12345)))
	first, second := len(storedItem(t, pq, 0)), len(storedItem(t, pq, 1))
	assert.Equal(t, int64(first+second), pq.Size())

	// The capacity check uses the same size: an item that would cross capacity is refused.
	full := createTestCompressedPersistentQueue(t, storagetest.NewMockStorageExtension(nil), request.SizerTypeBytes, int64(first+second-1), StorageCompressionZstd)
	require.NoError(t, full.Offer(context.Background(), intRequest(7)))
	require.ErrorIs(t, full.Offer(context.Background(), intRequest(12345)), ErrQueueIsFull)
	require.NoError(t, full.Shutdown(context.Background()))

	require.True(t, consume(pq, func(context.Context, intRequest) error { return nil }))
	assert.Equal(t, int64(second), pq.Size())
	require.True(t, consume(pq, func(context.Context, intRequest) error { return nil }))
	assert.Equal(t, int64(0), pq.Size())
	require.NoError(t, pq.Shutdown(context.Background()))
}

func TestPersistentQueue_CompressedItemsSurviveRestart(t *testing.T) {
	ext := storagetest.NewMockStorageExtension(nil)
	pq := createTestCompressedPersistentQueue(t, ext, request.SizerTypeBytes, 1000, StorageCompressionZstd)
	require.NoError(t, pq.Offer(context.Background(), intRequest(40)))
	require.NoError(t, pq.Offer(context.Background(), intRequest(20)))
	sizeBefore := pq.Size()
	require.True(t, consume(pq, func(context.Context, intRequest) error {
		return experr.NewShutdownErr(nil)
	}))
	require.NoError(t, pq.Shutdown(context.Background()))

	newPQ := createTestCompressedPersistentQueue(t, ext, request.SizerTypeBytes, 1000, StorageCompressionZstd)
	assert.Equal(t, sizeBefore, newPQ.Size())
	// The dispatched item moved to the back with its stored value unchanged.
	assert.True(t, bytes.HasPrefix(storedItem(t, newPQ, 2), zstdFrameMagic))
	for _, want := range []intRequest{20, 40} {
		require.True(t, consume(newPQ, func(_ context.Context, got intRequest) error {
			assert.Equal(t, want, got)
			return nil
		}))
	}
	assert.Equal(t, int64(0), newPQ.Size())
	require.NoError(t, newPQ.Shutdown(context.Background()))
}

func TestPersistentQueue_CompressionCanBeToggledOnAStoredQueue(t *testing.T) {
	ext := storagetest.NewMockStorageExtension(nil)
	plain := createTestCompressedPersistentQueue(t, ext, request.SizerTypeItems, 1000, StorageCompressionNone)
	require.NoError(t, plain.Offer(context.Background(), intRequest(1)))
	require.NoError(t, plain.Shutdown(context.Background()))

	compressed := createTestCompressedPersistentQueue(t, ext, request.SizerTypeItems, 1000, StorageCompressionZstd)
	require.NoError(t, compressed.Offer(context.Background(), intRequest(2)))
	assert.False(t, bytes.HasPrefix(storedItem(t, compressed, 0), zstdFrameMagic))
	assert.True(t, bytes.HasPrefix(storedItem(t, compressed, 1), zstdFrameMagic))
	require.NoError(t, compressed.Shutdown(context.Background()))

	plainAgain := createTestCompressedPersistentQueue(t, ext, request.SizerTypeItems, 1000, "")
	for _, want := range []intRequest{1, 2} {
		require.True(t, consume(plainAgain, func(_ context.Context, got intRequest) error {
			assert.Equal(t, want, got)
			return nil
		}))
	}
	assert.Equal(t, int64(0), plainAgain.Size())
	require.NoError(t, plainAgain.Shutdown(context.Background()))
}
