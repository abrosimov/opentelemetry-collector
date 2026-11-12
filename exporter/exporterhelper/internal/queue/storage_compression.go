// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queue"

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// StorageCompression selects how a persistent queue compresses the items it stores.
type StorageCompression string

const (
	// StorageCompressionNone stores items as the encoding marshals them. The empty value means the same.
	StorageCompressionNone StorageCompression = "none"
	// StorageCompressionZstd stores every new item as one zstd frame.
	StorageCompressionZstd StorageCompression = "zstd"
)

// Validate checks that the value names a supported compression.
func (c StorageCompression) Validate() error {
	switch c {
	case "", StorageCompressionNone, StorageCompressionZstd:
		return nil
	default:
		return fmt.Errorf("unsupported storage compression %q", string(c))
	}
}

// Enabled reports whether new items are compressed.
func (c StorageCompression) Enabled() bool {
	return c == StorageCompressionZstd
}

// The four bytes that open every zstd frame. A request encoded by pdata starts with the 0x0D tag of its
// format version, so a stored zstd frame and an uncompressed item cannot be confused.
var zstdFrameMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// maxDecodedItemSize bounds the memory one decompressed item may take, so that a corrupt or hostile frame
// header cannot allocate without limit.
const maxDecodedItemSize = 1 << 30

var (
	zstdEncoder = mustNewZstdEncoder()
	zstdDecoder = mustNewZstdDecoder()
)

func mustNewZstdEncoder() *zstd.Encoder {
	// A nil writer is allowed when only EncodeAll is used; EncodeAll is safe for concurrent use.
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		panic(err)
	}
	return encoder
}

func mustNewZstdDecoder() *zstd.Decoder {
	// DecodeAll is safe for concurrent use.
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxDecodedItemSize))
	if err != nil {
		panic(err)
	}
	return decoder
}

// compressStoredItem compresses the encoded item when compression is enabled.
func compressStoredItem(compression StorageCompression, encoded []byte) []byte {
	if !compression.Enabled() {
		return encoded
	}
	return zstdEncoder.EncodeAll(encoded, make([]byte, 0, len(encoded)/4))
}

// decompressStoredItem returns the encoded item from its stored form. It does not depend on the
// configured compression, so a queue written with compression is still read after compression is
// turned off, and items stored before compression was turned on are read unchanged.
//
// A value that opens with the zstd magic but does not decode is returned as stored: a custom encoding
// may produce those bytes, and a corrupt frame then fails in the encoding's own Unmarshal instead.
func decompressStoredItem(stored []byte) []byte {
	if !bytes.HasPrefix(stored, zstdFrameMagic) {
		return stored
	}
	decoded, err := zstdDecoder.DecodeAll(stored, nil)
	if err != nil {
		return stored
	}
	return decoded
}
