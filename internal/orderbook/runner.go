package orderbook

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/buffer"
	"github.com/market-data/internal/exchange"
)

func Run(orderBookManager *Manager, exchange exchange.Exchange, symbol string) {
	bufferMgr := buffer.NewBufferManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		exchange.BufferEvents(ctx, bufferMgr, strings.ToLower(symbol))
	}()

	go func() {
		if err := <-bufferMgr.ErrChan; err != nil {
			log.Printf("Exchange: %v Symbol: %v Message: bufferEvents error: %v", exchange.Name(), symbol, err)
		}
	}()

	for {
		time.Sleep(3 * time.Second)

		if bufferMgr.IsEmpty() {
			log.Printf("Exchange: %v Symbol: %v WARNING: No depth updates received yet, skipping this cycle.\n", exchange.Name(), symbol)
			continue
		}

		// Step 3: Get a new snapshot
		snapshot, err := exchange.GetDepthSnapshot(strings.ToUpper(symbol), 1000) // we do this to maintain correctness
		if err != nil {
			log.Printf("Exchange: %v Symbol: %v ERROR: Failed to get depth snapshot: %v", exchange.Name(), symbol, err)
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
			log.Printf("Exchange: %v Symbol: %v WARNING: All buffered events are older than snapshot lastUpdateId; waiting for fresh diffs...\n", exchange.Name(), symbol)
			continue
		}

		first := bufferMgr.GetFirst()
		if first == nil {
			log.Printf("Exchange: %v Symbol: %v WARNING: Buffer became empty, skipping this cycle.\n", exchange.Name(), symbol)
			continue
		}

		bufferFirstUpdateID := *first.U

		if *snapshotLastID < bufferFirstUpdateID {
			log.Printf(
				"Exchange: %v Symbol: %v WARNING: Snapshot (%d) is behind buffer (%d), restarting...",
				exchange.Name(),
				symbol,
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
				exchange.BufferEvents(ctx, bufferMgr, strings.ToLower(symbol))
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
				log.Printf("Exchange: %v Symbol: %v WARNING: New buffer still empty; retrying...", exchange.Name(), symbol)
				continue
			}

			continue
		}

		// Step 6: Set local order book to snapshot
		orderBook := &OrderBook{
			Bids:       snapshot.Bids,
			Asks:       snapshot.Asks,
			Identifier: Identifier{exchange.Name(), strings.ToUpper(symbol)},
		}

		orderBookManager.Set(orderBook)

		localUpdateID := *snapshotLastID

		// Step 7: Apply buffered events sequentially
		localUpdateID, success := orderBook.ApplyBufferedEvents(bufferCopy, localUpdateID)

		if !success {
			log.Printf("Exchange: %v Symbol: %v WARNING: Local order book desynced. Restarting synchronization.\n", exchange.Name(), symbol)
			continue
		}

		bufferMgr.RemoveOldEvents(localUpdateID)

		log.Printf(
			"Exchange: %v Symbol: %v Local book synced: %d bids / %d asks.",
			exchange.Name(),
			symbol,
			len(orderBook.Bids),
			len(orderBook.Asks),
		)

		log.Printf(
			"Exchange: %v Symbol: %v Current Order Books Length: %v",
			exchange.Name(),
			symbol,
			len(orderBookManager.OrderBooks),
		)

		time.Sleep(1 * time.Second)
	}
}
