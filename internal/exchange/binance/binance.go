package binance

import (
	"context"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/constants"
)

type Binance struct {
	client *client.BinanceSpotClient
}

func New() *Binance {
	return &Binance{
		client: NewSpotClient(),
	}
}

func (b *Binance) Name() string {
	return string(constants.ExchangeBinance)
}

func (b *Binance) Start() error {
	return b.client.WebsocketStreams.Connect()
}

func (b *Binance) SubscribeDepth(ctx context.Context, symbol string, out chan models.DiffBookDepthResponse, errCh chan<- error) error {
	handler, err := b.client.WebsocketStreams.WebSocketStreamsAPI.
		DiffBookDepth().
		Symbol(symbol).
		Execute()
	if err != nil {
		return err
	}

	handler.On("message", func(msg models.DiffBookDepthResponse) {
		select {
		case <-ctx.Done():
			return
		case out <- msg:
		}
	})

	handler.OnError(func(err error) {
		select {
		case errCh <- err:
		default:
		}
	})

	return nil
}
