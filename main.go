package main

import (
	"github.com/market-data/internal/binance"
	"github.com/market-data/internal/orderbook"
)

func main() {

	orderBookManager := orderbook.NewManager()
	binanceExchange := binance.New()

	go orderbook.Run(orderBookManager, binanceExchange, "BNBUSDT")
	go orderbook.Run(orderBookManager, binanceExchange, "ETHUSDT")

	select {}
}
