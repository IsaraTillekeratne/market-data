package binance

import (
	"context"
	"log"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/buffer"
)

func BufferEvents(ctx context.Context, wsClient *client.BinanceSpotClient, bufferMgr *buffer.Manager, symbol string) {
	err := wsClient.WebsocketStreams.Connect()
	if err != nil {
		bufferMgr.ErrChan <- err
		return
	}

	handler, err := wsClient.WebsocketStreams.WebSocketStreamsAPI.DiffBookDepth().
		Symbol(symbol).
		Execute()
	if err != nil {
		bufferMgr.ErrChan <- err
		return
	}

	handler.On("message", func(message models.DiffBookDepthResponse) {
		bufferMgr.Append(message)
	})

	<-ctx.Done()
	log.Println("bufferEvents: Context cancelled, shutting down...")
}
