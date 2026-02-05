package app

import (
	"log"
	"net/http"
	"sync"

	"github.com/market-data/internal/constants"
	"github.com/market-data/internal/exchange/binance"
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
	err := binanceExchange.Start()
	if err != nil {
		return err
	}

	hub := ws.NewHub(a.OrderBookManager, binanceExchange.Name())
	var wg sync.WaitGroup
	wg.Add(len(a.config.Symbols))

	for _, symbol := range a.config.Symbols {
		log.Printf("Order Book runner is being set up for symbol: %s\n", symbol)
		go orderbook.Run(a.OrderBookManager, binanceExchange, symbol, hub, &wg)
	}

	wg.Wait() // blocks until all order books are ready

	log.Println("All Order Books are synced. Starting the Websocket Server...")

	handler := ws.New(hub)

	addr := ":" + a.config.Port
	log.Printf("WebSocket server running on %s\n", addr)
	err = http.ListenAndServe(addr, handler)
	if err != nil {
		return err
	}

	return nil
}
