package orderbook

import "github.com/market-data/internal/data"

type Publisher interface {
	Publish(ev data.DepthUpdate)
}
