package main

import (
	"github.com/market-data/internal/binance"
	"github.com/market-data/internal/orderbook"
)

func main() {

	binanceExchange := binance.New()
	go orderbook.Run(binanceExchange, "BNBUSDT")
	go orderbook.Run(binanceExchange, "ETHUSDT")

	select {}
}
