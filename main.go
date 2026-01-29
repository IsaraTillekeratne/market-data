package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/binance/binance-connector-go/common/common"
)

type DepthSnapshot struct {
	LastUpdateId *int64
	Bids         map[string]string
	Asks         map[string]string
}

type OrderBook struct {
	Bids map[string]string
	Asks map[string]string
}

type BufferManager struct {
	buffer     []models.DiffBookDepthResponse
	bufferLock sync.Mutex
	errChan    chan error
}

func NewBufferManager() *BufferManager {
	return &BufferManager{
		buffer:  make([]models.DiffBookDepthResponse, 0),
		errChan: make(chan error, 1),
	}
}

func (bm *BufferManager) GetBuffer() []models.DiffBookDepthResponse {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()

	result := make([]models.DiffBookDepthResponse, len(bm.buffer))
	copy(result, bm.buffer)
	return result
}

func (bm *BufferManager) GetFirst() *models.DiffBookDepthResponse {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()

	if len(bm.buffer) == 0 {
		return nil
	}
	return &bm.buffer[0]
}

func (bm *BufferManager) IsEmpty() bool {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()
	return len(bm.buffer) == 0
}

func (bm *BufferManager) Len() int {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()
	return len(bm.buffer)
}

func (bm *BufferManager) Clear() {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()
	bm.buffer = make([]models.DiffBookDepthResponse, 0)
}

func (bm *BufferManager) append(message models.DiffBookDepthResponse) {
	bm.bufferLock.Lock()
	bm.buffer = append(bm.buffer, message)
	bm.bufferLock.Unlock()
}

func (bm *BufferManager) RemoveOldEvents(localUpdateID int64) {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()

	filtered := make([]models.DiffBookDepthResponse, 0)
	for _, ev := range bm.buffer {
		if ev.Smallu != nil && *ev.Smallu > localUpdateID {
			filtered = append(filtered, ev)
		}
	}
	bm.buffer = filtered
}

func main() {
	spotClient := client.NewBinanceSpotClient(
		client.WithRestAPI(common.NewConfigurationRestAPI()),
		client.WithWebsocketStreams(
			common.NewConfigurationWebsocketStreams(),
		),
	)

	localOrderBook(spotClient)
}

func bufferEvents(ctx context.Context, wsClient *client.BinanceSpotClient, bufferMgr *BufferManager) {
	err := wsClient.WebsocketStreams.Connect()
	if err != nil {
		bufferMgr.errChan <- err
		return
	}

	handler, err := wsClient.WebsocketStreams.WebSocketStreamsAPI.DiffBookDepth().
		Symbol("bnbusdt").
		Execute()
	if err != nil {
		bufferMgr.errChan <- err
		return
	}

	handler.On("message", func(message models.DiffBookDepthResponse) {
		bufferMgr.append(message)
	})

	<-ctx.Done()
	log.Println("bufferEvents: Context cancelled, shutting down...")
}

func depthSnapshot(client *client.BinanceSpotClient) (DepthSnapshot, error) {
	resp, err := client.RestApi.MarketAPI.Depth(context.Background()).
		Symbol("BNBUSDT").
		Limit(1000).
		Execute()

	if err != nil {
		return DepthSnapshot{}, err
	}

	bids := make(map[string]string)
	for _, bid := range resp.Data.Bids {
		price := bid[0]
		qty := bid[1]
		bids[price] = qty
	}

	asks := make(map[string]string)
	for _, ask := range resp.Data.Asks {
		price := ask[0]
		qty := ask[1]
		asks[price] = qty
	}

	response := DepthSnapshot{
		LastUpdateId: resp.Data.LastUpdateId,
		Bids:         bids,
		Asks:         asks,
	}

	return response, nil
}

func ApplyBufferedEvents(orderBook *OrderBook, buffer []models.DiffBookDepthResponse, localUpdateID int64) (int64, bool) {
	applied := 0

	for _, event := range buffer {
		if event.U == nil || event.Smallu == nil {
			log.Println("WARNING: Event missing U or u field, skipping")
			continue
		}

		eventU := *event.U
		eventSmallu := *event.Smallu

		// Step 7: Skip if event.u < local_update_id
		if eventSmallu < localUpdateID {
			continue
		}

		if eventU > localUpdateID+1 {
			log.Printf("WARNING: Gap detected between events (event.U=%d, local_update_id=%d). Resync required.",
				eventU, localUpdateID)
			return localUpdateID, false
		}

		ApplyUpdate(orderBook, event)
		localUpdateID = eventSmallu
		applied++
	}

	log.Printf("Applied %d buffered events. Local order book now synced to %d.", applied, localUpdateID)
	return localUpdateID, true
}

func ApplyUpdate(orderBook *OrderBook, event models.DiffBookDepthResponse) {
	for _, bid := range event.B {
		if len(bid) >= 2 {
			price := bid[0]
			qty := bid[1]

			qtyFloat, _ := strconv.ParseFloat(qty, 64)
			if qtyFloat == 0 {
				delete(orderBook.Bids, price)
			} else {
				orderBook.Bids[price] = qty
			}
		}
	}

	for _, ask := range event.A {
		if len(ask) >= 2 {
			price := ask[0]
			qty := ask[1]

			qtyFloat, _ := strconv.ParseFloat(qty, 64)
			if qtyFloat == 0 {
				delete(orderBook.Asks, price)
			} else {
				orderBook.Asks[price] = qty
			}
		}
	}
}

func localOrderBook(client *client.BinanceSpotClient) {
	bufferMgr := NewBufferManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bufferEvents(ctx, client, bufferMgr)
	}()

	go func() {
		if err := <-bufferMgr.errChan; err != nil {
			log.Printf("bufferEvents error: %v", err)
		}
	}()

	for {
		time.Sleep(3 * time.Second)

		if bufferMgr.IsEmpty() {
			log.Println("WARNING: No depth updates received yet, skipping this cycle.")
			continue
		}

		// Step 3: Get a new snapshot
		snapshot, err := depthSnapshot(client)
		if err != nil {
			log.Printf("ERROR: Failed to get depth snapshot: %v", err)
			continue
		}

		snapshotLastID := snapshot.LastUpdateId

		// Step 5: Filter events where u > snapshot_last_id
		bufferCopy := bufferMgr.GetBuffer()
		filtered := make([]models.DiffBookDepthResponse, 0)
		for _, ev := range bufferCopy {
			if ev.Smallu != nil && *ev.Smallu > *snapshotLastID {
				filtered = append(filtered, ev)
			}
		}

		if len(filtered) == 0 {
			log.Println("WARNING: All buffered events are older than snapshot lastUpdateId; waiting for fresh diffs...")
			continue
		}

		first := bufferMgr.GetFirst()
		if first == nil {
			log.Println("WARNING: Buffer became empty, skipping this cycle.")
			continue
		}

		bufferFirstUpdateID := *first.U

		if *snapshotLastID < bufferFirstUpdateID {
			log.Printf(
				"WARNING: Snapshot (%d) is behind buffer (%d), restarting...",
				snapshotLastID,
				bufferFirstUpdateID,
			)

			cancel()
			wg.Wait()

			bufferMgr.Clear()
			ctx, cancel = context.WithCancel(context.Background())
			wg.Add(1)
			go func() {
				defer wg.Done()
				bufferEvents(ctx, client, bufferMgr)
			}()

			bufferReady := false
			for i := 0; i < 10; i++ {
				time.Sleep(1 * time.Second)
				if !bufferMgr.IsEmpty() {
					bufferReady = true
					break
				}
			}

			if !bufferReady {
				log.Println("WARNING: New buffer still empty; retrying...")
				continue
			}

			continue
		}

		// Step 6: Set local order book to snapshot
		orderBook := OrderBook{
			Bids: snapshot.Bids,
			Asks: snapshot.Asks,
		}
		localUpdateID := *snapshotLastID

		// Step 7: Apply buffered events sequentially
		localUpdateID, success := ApplyBufferedEvents(&orderBook, bufferCopy, localUpdateID)

		if !success {
			log.Println("WARNING: Local order book desynced. Restarting synchronization.")
			continue
		}

		bufferMgr.RemoveOldEvents(localUpdateID)

		log.Printf(
			"Local book synced: %d bids / %d asks.",
			len(orderBook.Bids),
			len(orderBook.Asks),
		)

		time.Sleep(1 * time.Second)
	}
}
