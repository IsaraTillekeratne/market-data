package main

import (
	"github.com/market-data/internal/binance"
	"github.com/market-data/internal/orderbook"
)

func main() {

	spotClient := binance.NewSpotClient()
	orderbook.Run(spotClient, "BNBUSDT")
}
