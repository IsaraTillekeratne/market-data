package binance

import (
	"context"
	"log"
	"sync"
	"time"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/constants"
)

type Binance struct {
	client           *client.BinanceSpotClient
	ConnectedChan    chan struct{}
	DisconnectedChan chan struct{}
	mu               sync.Mutex
}

func (b *Binance) GetDisconnectedChan() chan struct{} {
	return b.DisconnectedChan
}

func (b *Binance) GetConnectedChan() chan struct{} {
	return b.ConnectedChan
}

func New() *Binance {
	return &Binance{
		client:           NewSpotClient(),
		ConnectedChan:    make(chan struct{}),
		DisconnectedChan: make(chan struct{}),
	}
}

func (b *Binance) Name() string {
	return string(constants.ExchangeBinance)
}

func (b *Binance) Start() {

	sleepTime := 3 * time.Second

	for {
		err := b.client.WebsocketStreams.Connect()
		if err != nil {
			log.Printf("Websocket connection error: %v\n", err)
			log.Println("Websocket connection Retrying...")
			time.Sleep(sleepTime)
			continue
		}

		// connection is successful
		log.Println("Websocket connection is successful")
		close(b.ConnectedChan)

		<-b.DisconnectedChan

		log.Println("Websocket disconnected. Reconnecting...")

		// Recreate channels for next cycle
		b.mu.Lock()
		b.ConnectedChan = make(chan struct{})
		b.DisconnectedChan = make(chan struct{})
		b.mu.Unlock()
	}

}

func (b *Binance) SubscribeDepth(ctx context.Context, symbol string, out chan models.DiffBookDepthResponse) error {

	handler, err := b.client.WebsocketStreams.WebSocketStreamsAPI.
		DiffBookDepth().
		Symbol(symbol).
		Execute()
	if err != nil {
		return err
	}

	// Capture the current disconnect channel instance
	b.mu.Lock()
	disconnectCh := b.DisconnectedChan
	b.mu.Unlock()

	handler.On("message", func(msg models.DiffBookDepthResponse) {
		select {
		case <-ctx.Done():
			return
		case out <- msg:
		}
	})

	// this identifies a disconnection
	handler.OnError(func(err error) {
		select {
		case <-disconnectCh: // already closed
		default:
			close(disconnectCh) // closes the disconnect channel to inform others
		}
	})

	return nil
}
