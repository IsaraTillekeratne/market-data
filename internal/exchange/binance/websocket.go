package binance

import (
	"context"
	"log"
	"strings"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
	"github.com/market-data/internal/buffer"
	"github.com/market-data/internal/data"
)

// Implementation in the Binance documentation
//func (b *Binance) BufferEvents(ctx context.Context, bufferMgr *buffer.Manager, symbol string) {
//	err := b.client.WebsocketStreams.Connect()
//	if err != nil {
//		bufferMgr.ErrChan <- err
//		return
//	}
//
//	handler, err := b.client.WebsocketStreams.WebSocketStreamsAPI.DiffBookDepth().
//		Symbol(symbol).
//		Execute()
//	if err != nil {
//		bufferMgr.ErrChan <- err
//		return
//	}
//
//	handler.On("message", func(message models.DiffBookDepthResponse) {
//		bufferMgr.Append(message)
//		log.Printf("WS EVENT: symbol=%s u=%d U=%d", *message.S, *message.Smallu, *message.U)
//	})
//
//	<-ctx.Done() // once signal is received through cancel(), this triggers
//	log.Println("bufferEvents: Context cancelled, shutting down...")
//}

func (b *Binance) BufferEvents(
	ctx context.Context,
	bufferMgr *buffer.Manager,
	symbol string,
	out chan<- data.DepthUpdate,
) {
	ch := make(chan models.DiffBookDepthResponse, 1000)

	if err := b.SubscribeDepth(ctx, symbol, ch); err != nil {
		bufferMgr.ErrChan <- err
		return
	}

	for {
		select {
		case msg := <-ch:
			bufferMgr.Append(msg)

			out <- data.DepthUpdate{
				Exchange: b.Name(),
				Symbol:   strings.ToUpper(symbol),
				Bids:     msg.B,
				Asks:     msg.A,
			}
		case <-ctx.Done():
			log.Printf("BufferEvents stopped for %s", symbol)
			return
		}
	}
}
