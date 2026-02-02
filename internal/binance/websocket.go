package binance

import (
	"context"
	"log"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/buffer"
)

func (binance *Binance) BufferEvents(ctx context.Context, bufferMgr *buffer.Manager, symbol string) {
	err := binance.client.WebsocketStreams.Connect()
	if err != nil {
		bufferMgr.ErrChan <- err
		return
	}

	handler, err := binance.client.WebsocketStreams.WebSocketStreamsAPI.DiffBookDepth().
		Symbol(symbol).
		Execute()
	if err != nil {
		bufferMgr.ErrChan <- err
		return
	}

	handler.On("message", func(message models.DiffBookDepthResponse) {
		bufferMgr.Append(message)
	})

	<-ctx.Done() // once signal is received through cancel(), this triggers
	log.Println("bufferEvents: Context cancelled, shutting down...")
}
