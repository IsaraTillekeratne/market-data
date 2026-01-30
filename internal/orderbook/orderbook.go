package orderbook

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/binance"
	"github.com/market-data/internal/buffer"
)

type OrderBook struct {
	Bids map[string]string
	Asks map[string]string
}

func Run(client *client.BinanceSpotClient, symbol string) {
	bufferMgr := buffer.NewBufferManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		binance.BufferEvents(ctx, client, bufferMgr, strings.ToLower(symbol))
	}()

	go func() {
		if err := <-bufferMgr.ErrChan; err != nil {
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
		snapshot, err := binance.GetDepthSnapshot(client, strings.ToUpper(symbol), 1000) // we do this to maintain correctness
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
				binance.BufferEvents(ctx, client, bufferMgr, strings.ToLower(symbol))
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
		localUpdateID, success := orderBook.ApplyBufferedEvents(bufferCopy, localUpdateID)

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

func (orderBook *OrderBook) ApplyBufferedEvents(buffer []models.DiffBookDepthResponse, localUpdateID int64) (int64, bool) {
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

		orderBook.ApplyUpdate(event)
		localUpdateID = eventSmallu
		applied++
	}

	log.Printf("Applied %d buffered events. Local order book now synced to %d.", applied, localUpdateID)
	return localUpdateID, true
}

func (orderBook *OrderBook) ApplyUpdate(event models.DiffBookDepthResponse) {
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
