package exchange

import (
	"context"

	"github.com/market-data/internal/buffer"
	"github.com/market-data/internal/data"
	"github.com/market-data/internal/snapshot"
)

type Exchange interface {
	Name() string
	Start() error
	GetDepthSnapshot(symbol string, limit int32) (snapshot.DepthSnapshot, error)
	BufferEvents(ctx context.Context, bufferMgr *buffer.Manager, symbol string, out chan<- data.DepthUpdate)
}
