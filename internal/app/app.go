package app

import (
	"log"
	"net/http"

	"github.com/market-data/internal/binance"
	"github.com/market-data/internal/constants"
	"github.com/market-data/internal/orderbook"
	"github.com/market-data/internal/ws"
)

type App struct {
	OrderBookManager *orderbook.Manager
	config           Config
}

func New(cfg Config) *App {
	return &App{
		OrderBookManager: orderbook.NewManager(),
		config:           cfg,
	}
}

func (a *App) Start() error {

	log.Printf("Exchange: %s is being set up...\n", constants.ExchangeBinance)
	binanceExchange := binance.New()

	for _, symbol := range a.config.Symbols {
		log.Printf("Order Book runner is being set up for symbol: %s\n", symbol)
		go orderbook.Run(a.OrderBookManager, binanceExchange, symbol)
	}

	hub := ws.NewHub()
	handler := ws.New(hub)

	addr := ":" + a.config.Port
	log.Printf("WebSocket server running on %s\n", addr)
	err := http.ListenAndServe(addr, handler)
	if err != nil {
		return err
	}

	return nil
}
