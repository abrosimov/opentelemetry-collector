// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queue"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/experr"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pipeline"
)

const (
	zapKey           = "key"
	zapNumberOfItems = "numberOfItems"

	legacyReadIndexKey                = "ri"
	legacyWriteIndexKey               = "wi"
	legacyCurrentlyDispatchedItemsKey = "di"

	// metadataKey is the new single key for all queue metadata.
	metadataKey = "qmv0"
)

var (
	errValueNotSet        = errors.New("value not set")
	errInvalidValue       = errors.New("invalid value")
	errNoStorageClient    = errors.New("no storage client extension found")
	errWrongExtensionType = errors.New("requested extension is not a storage extension")
)

// readRetryInterval is how long a reader waits before it tries again to dispatch an item whose dispatch
// could not be persisted. A variable so that tests can shorten it.
var readRetryInterval = time.Second

var indexDonePool = sync.Pool{
	New: func() any {
		return &indexDone{}
	},
}

// persistentQueue provides a persistent queue implementation backed by file storage extension
//
// Write index describes the position at which next item is going to be stored.
// Read index describes which item needs to be read next.
// When Write index = Read index, no elements are in the queue.
//
// The items currently dispatched by consumers are not deleted until the processing is finished.
// Their list is stored under a separate key.
//
//	┌───────file extension-backed queue───────┐
//	│                                         │
//	│     ┌───┐     ┌───┐ ┌───┐ ┌───┐ ┌───┐   │
//	│ n+1 │ n │ ... │ 4 │ │ 3 │ │ 2 │ │ 1 │   │
//	│     └───┘     └───┘ └─x─┘ └─|─┘ └─x─┘   │
//	│                       x     |     x     │
//	└───────────────────────x─────|─────x─────┘
//	   ▲              ▲     x     |     x
//	   │              │     x     |     xxxx deleted
//	   │              │     x     |
//	 write          read    x     └── currently dispatched item
//	 index          index   x
//	                        xxxx deleted
type persistentQueue[T request.Request] struct {
	logger      *zap.Logger
	client      storage.Client
	encoding    Encoding[T]
	capacity    int64
	sizerType   request.SizerType
	activeSizer request.Sizer
	itemsSizer  request.Sizer
	storageID   component.ID
	id          component.ID
	signal      pipeline.Signal
	stopCh      chan struct{}
	stopOnce    sync.Once

	// mu guards everything declared below.
	mu              sync.Mutex
	hasMoreElements *sync.Cond
	hasMoreSpace    *cond
	metadata        PersistentMetadata
	refClient       int64
	stopped         bool
	readFailures    int

	blockOnOverflow bool
}

// newPersistentQueue creates a new queue backed by file storage; name and signal must be a unique combination that identifies the queue storage
func newPersistentQueue[T request.Request](set Settings[T]) readableQueue[T] {
	pq := &persistentQueue[T]{
		logger:          set.Telemetry.Logger,
		encoding:        set.Encoding,
		capacity:        set.Capacity,
		sizerType:       set.SizerType,
		activeSizer:     request.NewSizer(set.SizerType),
		itemsSizer:      request.NewItemsSizer(),
		storageID:       *set.StorageID,
		id:              set.ID,
		signal:          set.Signal,
		stopCh:          make(chan struct{}),
		blockOnOverflow: set.BlockOnOverflow,
	}
	pq.hasMoreElements = sync.NewCond(&pq.mu)
	pq.hasMoreSpace = newCond(&pq.mu)
	return pq
}

// Start starts the persistentQueue with the given number of consumers.
func (pq *persistentQueue[T]) Start(ctx context.Context, host component.Host) error {
	storageClient, err := toStorageClient(ctx, pq.storageID, host, pq.id, pq.signal)
	if err != nil {
		return err
	}
	pq.initClient(ctx, storageClient)
	return nil
}

func (pq *persistentQueue[T]) Size() int64 {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.internalSize()
}

func (pq *persistentQueue[T]) internalSize() int64 {
	switch pq.sizerType {
	case request.SizerTypeBytes:
		return pq.metadata.BytesSize
	case request.SizerTypeItems:
		return pq.metadata.ItemsSize
	default:
		return pq.requestSize()
	}
}

func (pq *persistentQueue[T]) requestSize() int64 {
	return int64(pq.metadata.WriteIndex-pq.metadata.ReadIndex) + int64(len(pq.metadata.CurrentlyDispatchedItems))
}

func (pq *persistentQueue[T]) Capacity() int64 {
	return pq.capacity
}

func (pq *persistentQueue[T]) initClient(ctx context.Context, client storage.Client) {
	pq.client = client
	// Start with a reference 1 which is the reference we use for the producer goroutines and initialization.
	pq.refClient = 1

	// Try to load from new consolidated metadata first
	err := pq.loadQueueMetadata(ctx)
	switch {
	case err == nil:
		pq.enqueueNotDispatchedReqs(ctx, pq.metadata.CurrentlyDispatchedItems)
	case !errors.Is(err, errValueNotSet):
		pq.logger.Error("Failed getting metadata, starting with new ones", zap.Error(err))
		pq.metadata = PersistentMetadata{}
	default:
		pq.logger.Info("New queue metadata key not found, attempting to load legacy format.")
		pq.loadLegacyMetadata(ctx)
	}
}

// loadQueueMetadata loads queue metadata from the consolidated key
func (pq *persistentQueue[T]) loadQueueMetadata(ctx context.Context) error {
	buf, err := pq.client.Get(ctx, metadataKey)
	if err != nil {
		return err
	}

	if len(buf) == 0 {
		return errValueNotSet
	}

	if err := proto.Unmarshal(buf, &pq.metadata); err != nil {
		return err
	}

	pq.logger.Info("Loaded queue metadata",
		zap.Uint64("readIndex", pq.metadata.ReadIndex),
		zap.Uint64("writeIndex", pq.metadata.WriteIndex),
		zap.Int64("itemsSize", pq.metadata.ItemsSize),
		zap.Int64("bytesSize", pq.metadata.BytesSize),
		zap.Int("dispatchedItems", len(pq.metadata.CurrentlyDispatchedItems)))

	return nil
}

// TODO: Remove legacy format support after 6 months (target: December 2025)
func (pq *persistentQueue[T]) loadLegacyMetadata(ctx context.Context) {
	// Fallback to legacy individual keys for backward compatibility
	riOp := storage.GetOperation(legacyReadIndexKey)
	wiOp := storage.GetOperation(legacyWriteIndexKey)

	err := pq.client.Batch(ctx, riOp, wiOp)
	if err == nil {
		pq.metadata.ReadIndex, err = bytesToItemIndex(riOp.Value)
	}

	if err == nil {
		pq.metadata.WriteIndex, err = bytesToItemIndex(wiOp.Value)
	}

	if err != nil {
		if errors.Is(err, errValueNotSet) {
			pq.logger.Info("Initializing new persistent queue")
		} else {
			pq.logger.Error("Failed getting read/write index, starting with new ones", zap.Error(err))
		}
		pq.metadata.ReadIndex = 0
		pq.metadata.WriteIndex = 0
	}

	pq.retrieveAndEnqueueNotDispatchedReqs(ctx)

	// Save to a new format and clean up legacy keys
	metadataBytes, err := proto.Marshal(&pq.metadata)
	if err != nil {
		pq.logger.Error("Failed to marshal metadata", zap.Error(err))
		return
	}

	if err = pq.client.Set(ctx, metadataKey, metadataBytes); err != nil {
		pq.logger.Error("Failed to persist current metadata to storage", zap.Error(err))
		return
	}

	if err = pq.client.Batch(ctx,
		storage.DeleteOperation(legacyReadIndexKey),
		storage.DeleteOperation(legacyWriteIndexKey),
		storage.DeleteOperation(legacyCurrentlyDispatchedItemsKey)); err != nil {
		pq.logger.Warn("Failed to cleanup legacy metadata keys", zap.Error(err))
	} else {
		pq.logger.Info("Successfully migrated to consolidated metadata format")
	}
}

func (pq *persistentQueue[T]) Shutdown(ctx context.Context) error {
	// If the queue is not initialized, there is nothing to shut down.
	if pq.client == nil {
		return nil
	}

	pq.mu.Lock()
	defer pq.mu.Unlock()
	// Mark this queue as stopped, so consumer don't start any more work.
	pq.stopped = true
	pq.stopOnce.Do(func() { close(pq.stopCh) })
	pq.hasMoreElements.Broadcast()
	return pq.unrefClient(ctx)
}

// unrefClient unrefs the client, and closes if no more references. Callers MUST hold the mutex.
// This is needed because consumers of the queue may still process the requests while the queue is shutting down or immediately after.
func (pq *persistentQueue[T]) unrefClient(ctx context.Context) error {
	pq.refClient--
	if pq.refClient == 0 {
		return pq.client.Close(ctx)
	}
	return nil
}

// Offer inserts the specified element into this queue if it is possible to do so immediately
// without violating capacity restrictions. If success returns no error.
// It returns ErrQueueIsFull if no space is currently available.
func (pq *persistentQueue[T]) Offer(ctx context.Context, req T) error {
	// Encode before taking the lock: the bytes size is the size of the encoded item, and encoding reads
	// nothing the lock guards.
	buf, err := pq.encode(ctx, req)
	if err != nil {
		return err
	}
	itemsSize := pq.itemsSizer.Sizeof(req)
	bytesSize := storedSize(buf)
	size := pq.activeSizeOf(req, itemsSize, bytesSize)

	pq.mu.Lock()
	defer pq.mu.Unlock()

	for pq.internalSize()+size > pq.capacity {
		if !pq.blockOnOverflow {
			return ErrQueueIsFull
		}
		if err := pq.hasMoreSpace.Wait(ctx); err != nil {
			return err
		}
	}

	pq.metadata.ItemsSize += itemsSize
	pq.metadata.BytesSize += bytesSize

	if err := pq.putInternal(ctx, buf); err != nil {
		pq.metadata.ItemsSize -= itemsSize
		pq.metadata.BytesSize -= bytesSize
		return err
	}
	return nil
}

// encode returns the form in which the request is stored.
func (pq *persistentQueue[T]) encode(ctx context.Context, req T) ([]byte, error) {
	return pq.encoding.Marshal(ctx, req)
}

// decode returns the request from its stored form.
func (pq *persistentQueue[T]) decode(stored []byte) (context.Context, T, error) {
	return pq.encoding.Unmarshal(stored)
}

// storedSize returns the size the bytes sizer counts for an item: the bytes it takes in storage. Measuring what
// is stored, rather than the request before encoding, puts `queue_size` in the unit a storage extension's size
// limit uses, whatever the encoding does to the request, including compressing it.
func storedSize(stored []byte) int64 {
	return int64(len(stored))
}

func (pq *persistentQueue[T]) activeSizeOf(req T, itemsSize, bytesSize int64) int64 {
	switch pq.sizerType {
	case request.SizerTypeBytes:
		return bytesSize
	case request.SizerTypeItems:
		return itemsSize
	default:
		return pq.activeSizer.Sizeof(req)
	}
}

// putInternal adds the encoded request to the storage without updating items/bytes sizes.
// On error the write index is left unchanged.
func (pq *persistentQueue[T]) putInternal(ctx context.Context, reqBuf []byte) error {
	pq.metadata.WriteIndex++
	metadataBuf, err := proto.Marshal(&pq.metadata)
	if err == nil {
		// Carry out a transaction where we both add the item and update the write index
		err = pq.client.Batch(ctx,
			storage.SetOperation(metadataKey, metadataBuf),
			storage.SetOperation(getItemKey(pq.metadata.WriteIndex-1), reqBuf))
	}
	if err != nil {
		// Anything the storage kept from this batch is overwritten by the next metadata write and the next put.
		pq.metadata.WriteIndex--
		return err
	}

	pq.hasMoreElements.Signal()

	return nil
}

func (pq *persistentQueue[T]) Read(ctx context.Context) (context.Context, T, Done, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for {
		if pq.stopped {
			var req T
			return context.Background(), req, nil, false
		}

		// Read until either a successful retrieved element or no more elements in the storage.
		var dispatchErr error
		for pq.metadata.ReadIndex != pq.metadata.WriteIndex {
			item, consumed, err := pq.getNextItem(ctx)
			if err != nil {
				dispatchErr = err
				break
			}
			pq.noteDispatchRecovered()
			// Ensure the used size are in sync when queue is drained.
			if pq.requestSize() == 0 {
				pq.metadata.BytesSize = 0
				pq.metadata.ItemsSize = 0
			}
			if consumed {
				id := indexDonePool.Get().(*indexDone)
				id.reset(item.index, pq.itemsSizer.Sizeof(item.req), item.bytesSize, pq)
				return item.ctx, item.req, id, true
			}
			// More space available, data was dropped.
			pq.hasMoreSpace.Signal()
		}

		if dispatchErr != nil {
			pq.waitAfterDispatchFailure(dispatchErr)
			continue
		}

		// TODO: Need to change the Queue interface to return an error to allow distinguish between shutdown and context canceled.
		//  Until then use the sync.Cond.
		pq.hasMoreElements.Wait()
	}
}

// waitAfterDispatchFailure releases the lock for readRetryInterval, or until the queue stops, after the
// dispatch of an item could not be persisted. The item stays at the read index, so the next attempt reads
// it again. Callers MUST hold the mutex.
func (pq *persistentQueue[T]) waitAfterDispatchFailure(err error) {
	pq.readFailures++
	if pq.readFailures == 1 {
		pq.logger.Error("Failed to persist the dispatch of a queue item; the item stays in the queue and is read again",
			zap.Error(err), zap.Duration("interval", readRetryInterval))
	} else {
		pq.logger.Debug("Failed again to persist the dispatch of a queue item",
			zap.Error(err), zap.Int("attempts", pq.readFailures))
	}
	pq.mu.Unlock()
	select {
	case <-time.After(readRetryInterval):
	case <-pq.stopCh:
	}
	pq.mu.Lock()
}

// noteDispatchRecovered logs the end of a run of failed dispatches. Callers MUST hold the mutex.
func (pq *persistentQueue[T]) noteDispatchRecovered() {
	if pq.readFailures == 0 {
		return
	}
	pq.logger.Info("Persisting the dispatch of queue items succeeded again", zap.Int("failedAttempts", pq.readFailures))
	pq.readFailures = 0
}

type dispatchedItem[T any] struct {
	index     uint64
	req       T
	ctx       context.Context
	bytesSize int64
}

// getNextItem pulls the next available item from the persistent storage along with its index. Once processing is
// finished, the index should be called with onDone to clean up the storage.
//
// It returns false without an error when the item could not be decoded and was dropped. It returns an error,
// and leaves the queue as it was, when the storage could not record the dispatch: the item is still stored
// and acknowledged, so dropping it would lose it while the storage is, for example, at its size limit.
func (pq *persistentQueue[T]) getNextItem(ctx context.Context) (dispatchedItem[T], bool, error) {
	index := pq.metadata.ReadIndex
	pq.metadata.ReadIndex++
	pq.metadata.CurrentlyDispatchedItems = append(pq.metadata.CurrentlyDispatchedItems, index)

	getOp := storage.GetOperation(getItemKey(index))
	metadataBytes, err := proto.Marshal(&pq.metadata)
	if err == nil {
		err = pq.client.Batch(ctx, storage.SetOperation(metadataKey, metadataBytes), getOp)
	}
	if err != nil {
		// The batch is one storage transaction, so the stored metadata is still the one before this read.
		pq.metadata.ReadIndex--
		pq.metadata.CurrentlyDispatchedItems = pq.metadata.CurrentlyDispatchedItems[:len(pq.metadata.CurrentlyDispatchedItems)-1]
		return dispatchedItem[T]{}, false, err
	}

	restoredCtx, req, err := pq.decode(getOp.Value)
	if err != nil {
		pq.logger.Debug("Failed to dispatch item", zap.Error(err))
		// We need to make sure that currently dispatched items list is cleaned
		if err = pq.itemDispatchingFinish(ctx, index); err != nil {
			pq.logger.Error("Error deleting item from queue", zap.Error(err))
		}

		return dispatchedItem[T]{}, false, nil
	}

	// Increase the reference count, so the client is not closed while the request is being processed.
	// The client cannot be closed because we hold the lock since last we checked `stopped`.
	pq.refClient++

	return dispatchedItem[T]{index: index, req: req, ctx: restoredCtx, bytesSize: storedSize(getOp.Value)}, true, nil
}

// onDone should be called to remove the item of the given index from the queue once processing is finished.
func (pq *persistentQueue[T]) onDone(index uint64, itemsSize, bytesSize int64, consumeErr error) {
	// Delete the item from the persistent storage after it was processed.
	pq.mu.Lock()
	// Always unref client even if the consumer is shutdown because we always ref it for every valid request.
	defer func() {
		if err := pq.unrefClient(context.Background()); err != nil {
			pq.logger.Error("Error closing the storage client", zap.Error(err))
		}
		pq.mu.Unlock()
	}()

	if experr.IsShutdownErr(consumeErr) {
		// The queue is shutting down, don't mark the item as dispatched, so it's picked up again after restart.
		// TODO: Handle partially delivered requests by updating their values in the storage.
		return
	}

	pq.metadata.BytesSize -= bytesSize
	if pq.metadata.BytesSize < 0 {
		pq.metadata.BytesSize = 0
	}
	pq.metadata.ItemsSize -= itemsSize
	if pq.metadata.ItemsSize < 0 {
		pq.metadata.ItemsSize = 0
	}

	if err := pq.itemDispatchingFinish(context.Background(), index); err != nil {
		pq.logger.Error("Error deleting item from queue", zap.Error(err))
	}

	// More space available after data are removed from the storage.
	pq.hasMoreSpace.Signal()
}

// retrieveAndEnqueueNotDispatchedReqs gets the items for which sending was not finished, cleans the storage
// and moves the items at the back of the queue.
func (pq *persistentQueue[T]) retrieveAndEnqueueNotDispatchedReqs(ctx context.Context) {
	var dispatchedItems []uint64

	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.logger.Debug("Checking if there are items left for dispatch by consumers")
	itemKeysBuf, err := pq.client.Get(ctx, legacyCurrentlyDispatchedItemsKey)
	if err == nil {
		dispatchedItems, err = bytesToItemIndexArray(itemKeysBuf)
	}
	if err != nil {
		pq.logger.Error("Could not fetch items left for dispatch by consumers", zap.Error(err))
		return
	}

	pq.enqueueNotDispatchedReqs(ctx, dispatchedItems)
}

// enqueueNotDispatchedReqs moves the items that were being dispatched when the queue last stopped to the back of
// the queue. Their sizes are still counted, because they were never finished. The move is one storage
// transaction: either every item is at its new index and the dispatched list is empty, or the storage is unchanged
// and the items are moved at the next start, so a crash at any point loses none of them.
func (pq *persistentQueue[T]) enqueueNotDispatchedReqs(ctx context.Context, dispatchedItems []uint64) {
	if len(dispatchedItems) == 0 {
		pq.logger.Debug("No items left for dispatch by consumers")
		return
	}

	pq.logger.Info("Fetching items left for dispatch by consumers", zap.Int(zapNumberOfItems,
		len(dispatchedItems)))
	retrieveBatch := make([]*storage.Operation, len(dispatchedItems))
	for i, it := range dispatchedItems {
		retrieveBatch[i] = storage.GetOperation(getItemKey(it))
	}
	if err := pq.client.Batch(ctx, retrieveBatch...); err != nil {
		pq.logger.Warn("Failed retrieving items left by consumers; they are moved at the next start", zap.Error(err))
		pq.metadata.CurrentlyDispatchedItems = dispatchedItems
		return
	}

	previousWriteIndex := pq.metadata.WriteIndex
	moveBatch := make([]*storage.Operation, 0, 2*len(retrieveBatch)+1)
	moved := 0
	for _, op := range retrieveBatch {
		moveBatch = append(moveBatch, storage.DeleteOperation(op.Key))
		if op.Value == nil {
			pq.logger.Warn("Failed retrieving item", zap.String(zapKey, op.Key), zap.Error(errValueNotSet))
			continue
		}
		if _, _, err := pq.decode(op.Value); err != nil {
			pq.logger.Warn("Failed unmarshalling item", zap.String(zapKey, op.Key), zap.Error(err))
			continue
		}
		// The stored value is moved as it is.
		moveBatch = append(moveBatch, storage.SetOperation(getItemKey(pq.metadata.WriteIndex), op.Value))
		pq.metadata.WriteIndex++
		moved++
	}
	pq.metadata.CurrentlyDispatchedItems = nil

	metadataBytes, err := proto.Marshal(&pq.metadata)
	if err == nil {
		err = pq.client.Batch(ctx, append(moveBatch, storage.SetOperation(metadataKey, metadataBytes))...)
	}
	if err != nil {
		pq.metadata.WriteIndex = previousWriteIndex
		pq.metadata.CurrentlyDispatchedItems = dispatchedItems
		pq.logger.Error("Failed moving items left by consumers back to queue; they are moved at the next start",
			zap.Int(zapNumberOfItems, len(dispatchedItems)), zap.Error(err))
		return
	}

	pq.logger.Info("Moved items for dispatching back to queue", zap.Int(zapNumberOfItems, moved))
}

// itemDispatchingFinish removes the item from the list of currently dispatched items and deletes it from the persistent queue
func (pq *persistentQueue[T]) itemDispatchingFinish(ctx context.Context, index uint64) error {
	lenCDI := len(pq.metadata.CurrentlyDispatchedItems)
	for i := range lenCDI {
		if pq.metadata.CurrentlyDispatchedItems[i] == index {
			pq.metadata.CurrentlyDispatchedItems[i] = pq.metadata.CurrentlyDispatchedItems[lenCDI-1]
			pq.metadata.CurrentlyDispatchedItems = pq.metadata.CurrentlyDispatchedItems[:lenCDI-1]
			break
		}
	}

	// Ensure the used size are in sync when queue is drained.
	if pq.requestSize() == 0 {
		pq.metadata.BytesSize = 0
		pq.metadata.ItemsSize = 0
	}

	metadataBytes, err := proto.Marshal(&pq.metadata)
	if err != nil {
		return err
	}

	setOp := storage.SetOperation(metadataKey, metadataBytes)
	deleteOp := storage.DeleteOperation(getItemKey(index))
	err = pq.client.Batch(ctx, setOp, deleteOp)
	if err == nil {
		// Everything ok, exit
		return nil
	}

	// got an error, try to gracefully handle it
	pq.logger.Warn("Failed updating currently dispatched items, trying to delete the item first",
		zap.Error(err))

	if err = pq.client.Batch(ctx, deleteOp); err != nil {
		// Return an error here, as this indicates an issue with the underlying storage medium
		return fmt.Errorf("failed deleting item from queue, got error from storage: %w", err)
	}

	if err = pq.client.Batch(ctx, setOp); err != nil {
		// even if this fails, we still have the right dispatched items in memory
		// at worst, we'll have the wrong list in storage, and we'll discard the nonexistent items during startup
		return fmt.Errorf("failed updating currently dispatched items, but deleted item successfully: %w", err)
	}

	return nil
}

func toStorageClient(ctx context.Context, storageID component.ID, host component.Host, ownerID component.ID, signal pipeline.Signal) (storage.Client, error) {
	ext, found := host.GetExtensions()[storageID]
	if !found {
		return nil, errNoStorageClient
	}

	storageExt, ok := ext.(storage.Extension)
	if !ok {
		return nil, errWrongExtensionType
	}

	return storageExt.GetClient(ctx, component.KindExporter, ownerID, signal.String())
}

func getItemKey(index uint64) string {
	return strconv.FormatUint(index, 10)
}

func bytesToItemIndex(buf []byte) (uint64, error) {
	if buf == nil {
		return uint64(0), errValueNotSet
	}
	// The sizeof uint64 in binary is 8.
	if len(buf) < 8 {
		return 0, errInvalidValue
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func bytesToItemIndexArray(buf []byte) ([]uint64, error) {
	if len(buf) == 0 {
		return nil, nil
	}

	// The sizeof uint32 in binary is 4.
	if len(buf) < 4 {
		return nil, errInvalidValue
	}
	size := int(binary.LittleEndian.Uint32(buf))
	if size == 0 {
		return nil, nil
	}

	buf = buf[4:]
	// The sizeof uint64 in binary is 8, so we need to have size*8 bytes.
	if len(buf) < size*8 {
		return nil, errInvalidValue
	}

	val := make([]uint64, size)
	for i := range size {
		val[i] = binary.LittleEndian.Uint64(buf)
		buf = buf[8:]
	}
	return val, nil
}

type indexDone struct {
	index     uint64
	itemsSize int64
	bytesSize int64
	queue     interface {
		onDone(uint64, int64, int64, error)
	}
}

func (id *indexDone) reset(index uint64, itemsSize, bytesSize int64, queue interface {
	onDone(uint64, int64, int64, error)
},
) {
	id.index = index
	id.itemsSize = itemsSize
	id.bytesSize = bytesSize
	id.queue = queue
}

func (id *indexDone) OnDone(err error) {
	id.queue.onDone(id.index, id.itemsSize, id.bytesSize, err)
}
