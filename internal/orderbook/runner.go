package orderbook

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/buffer"
	"github.com/market-data/internal/data"
	"github.com/market-data/internal/exchange"
)

type systemState int

const (
	stateInitial systemState = iota
	stateDisconnected
	stateResynced
	stateLive
)

func Run(orderBookManager *Manager, exchange exchange.Exchange, symbol string, publisher Publisher, readyWG *sync.WaitGroup) {

	state := stateInitial
	var stateLock sync.Mutex

	var isInitiallySynced = false

	bufferMgr := buffer.NewBufferManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan data.DepthUpdate, 1000)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		exchange.BufferEvents(ctx, bufferMgr, strings.ToLower(symbol), updates)
	}()

	go func() {
		if err := <-bufferMgr.ErrChan; err != nil {
			log.Printf("Exchange: %v Symbol: %v Message: bufferEvents error: %v", exchange.Name(), symbol, err)
		}
	}()

	go func() {
		for ev := range updates {
			publisher.Publish(ev)
		}
	}()

	// handle disconnection
	go handleDisconnection(exchange, symbol, publisher, &state, &wg, bufferMgr, cancel, updates, ctx, &stateLock)

	for {
		time.Sleep(3 * time.Second)

		if state == stateDisconnected {
			log.Printf("Exchange: %v Symbol: %v WARNING: Websocket is disconnected, skipping this cycle.\n", exchange.Name(), symbol)
			continue
		}

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

			// start the resyncing process

			cancel() // signals the go routine to finish
			wg.Wait()

			bufferMgr.Clear()
			ctx, cancel = context.WithCancel(context.Background())
			wg.Add(1)
			go func() {
				defer wg.Done()
				exchange.BufferEvents(ctx, bufferMgr, strings.ToLower(symbol), updates)
			}()

			bufferReady := false
			for i := 0; i < 10; i++ {
				time.Sleep(1 * time.Second)
				if !bufferMgr.IsEmpty() {
					bufferReady = true
					break
				}
			}

			setState(&state, stateResynced, symbol, publisher, &stateLock)

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

		localUpdateID := *snapshotLastID

		// Step 7: Apply buffered events sequentially
		localUpdateID, success := orderBook.ApplyBufferedEvents(bufferCopy, localUpdateID)

		if !success {
			log.Printf("Exchange: %v Symbol: %v WARNING: Local order book desynced. Restarting synchronization.\n", exchange.Name(), symbol)
			continue
		}

		bufferMgr.RemoveOldEvents(localUpdateID)

		orderBookManager.Set(orderBook)
		if state != stateDisconnected { // to prevent state change from Disconnected -> Live
			setState(&state, stateLive, symbol, publisher, &stateLock)
		}

		log.Printf(
			"Exchange: %v Symbol: %v Local book synced: %d bids / %d asks.",
			exchange.Name(),
			symbol,
			len(orderBook.Bids),
			len(orderBook.Asks),
		)

		if !isInitiallySynced {
			log.Printf("Exchange: %v Symbol: %v is synced and ready for the first time",
				exchange.Name(),
				symbol)
			isInitiallySynced = true
			readyWG.Done()
		}

		time.Sleep(1 * time.Second)
	}
}

func setState(currentState *systemState, newState systemState, symbol string, publisher Publisher, stateLock *sync.Mutex) {
	if *currentState == newState {
		return
	}

	stateLock.Lock()
	*currentState = newState
	stateLock.Unlock()

	switch newState {
	case stateDisconnected:
		publisher.PublishSystem(symbol, "DISCONNECTED")
	case stateResynced:
		publisher.PublishSystem(symbol, "RESYNCED")
	case stateLive:
		publisher.PublishSystem(symbol, "LIVE")
	default:
	}
}

func handleDisconnection(exchange exchange.Exchange, symbol string, publisher Publisher, state *systemState,
	wg *sync.WaitGroup, bufferMgr *buffer.Manager, cancel context.CancelFunc, updates chan data.DepthUpdate, ctx context.Context, stateLock *sync.Mutex) {

	for {

		disconnectedChan := exchange.GetDisconnectedChan()
		connectedChan := exchange.GetConnectedChan()

		<-disconnectedChan
		setState(state, stateDisconnected, symbol, publisher, stateLock)

		<-connectedChan

		// start the resyncing process
		cancel() // clears up old handlers
		wg.Wait()

		bufferMgr.Clear()
		ctx, cancel = context.WithCancel(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			exchange.BufferEvents(ctx, bufferMgr, strings.ToLower(symbol), updates)
		}()

		bufferReady := false
		for i := 0; i < 10; i++ {
			time.Sleep(1 * time.Second)
			if !bufferMgr.IsEmpty() {
				bufferReady = true
				break
			}
		}

		log.Printf("Resynced after Disconnection. Buffer Ready:%v\n", bufferReady)
		setState(state, stateResynced, symbol, publisher, stateLock)
	}
}
