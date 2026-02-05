package orderbook

type Publisher interface {
	Publish(ob *OrderBook)
}
