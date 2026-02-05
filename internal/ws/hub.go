package ws

import (
	"encoding/json"
	"sync"

	"github.com/market-data/internal/data"
	"github.com/market-data/internal/orderbook"
)

type Hub struct {
	hubLock          sync.Mutex
	clients          map[*Client]struct{} // struct{} is used since, no need to store the value
	subscriptions    map[string]map[*Client]struct{}
	orderBookManager *orderbook.Manager
	exchange         string
}

func NewHub(obm *orderbook.Manager, exchange string) *Hub {
	return &Hub{
		clients:          make(map[*Client]struct{}),
		subscriptions:    make(map[string]map[*Client]struct{}),
		orderBookManager: obm,
		exchange:         exchange,
	}
}

func (h *Hub) Add(c *Client) {
	h.hubLock.Lock()
	defer h.hubLock.Unlock()
	h.clients[c] = struct{}{}
}

func (h *Hub) Remove(c *Client) {
	h.hubLock.Lock()
	defer h.hubLock.Unlock()

	for symbol := range c.subscriptions {
		delete(h.subscriptions[symbol], c)
	}

	delete(h.clients, c)
}

func (h *Hub) Broadcast(msg []byte) {
	h.hubLock.Lock()
	defer h.hubLock.Unlock()

	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			delete(h.clients, c)
			close(c.send)
		}
	}
}

func (h *Hub) Subscribe(symbol string, client *Client) {
	h.hubLock.Lock()
	defer h.hubLock.Unlock()

	if _, ok := h.subscriptions[symbol]; !ok {
		h.subscriptions[symbol] = make(map[*Client]struct{})
	}

	h.subscriptions[symbol][client] = struct{}{}
	client.subscriptions[symbol] = struct{}{}

}

func (h *Hub) Unsubscribe(symbol string, client *Client) {
	h.hubLock.Lock()
	defer h.hubLock.Unlock()

	delete(client.subscriptions, symbol)
	if subs, ok := h.subscriptions[symbol]; ok {
		delete(subs, client)
	}
}

func (h *Hub) Publish(ev data.DepthUpdate) {
	h.hubLock.Lock()
	defer h.hubLock.Unlock()

	subscribedClients := h.subscriptions[ev.Symbol]

	msg := map[string]interface{}{
		"type":   "update",
		"symbol": ev.Symbol,
		"bids":   ev.Bids,
		"asks":   ev.Asks,
	}

	responseData, _ := json.Marshal(msg)

	for c := range subscribedClients {
		select {
		case c.send <- responseData:
		default:
			// slow client → drop
		}
	}
}
