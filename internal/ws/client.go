package ws

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/market-data/internal/orderbook"
	"github.com/market-data/internal/ws/messages"
)

type Client struct {
	ID            string
	connection    *websocket.Conn
	send          chan []byte
	hub           *Hub
	subscriptions map[string]struct{}
}

func NewClient(connection *websocket.Conn, hub *Hub) *Client {
	return &Client{
		ID:            uuid.New().String(),
		connection:    connection,
		send:          make(chan []byte, 256),
		hub:           hub,
		subscriptions: make(map[string]struct{}),
	}
}

func (c *Client) readLoop() {
	defer func() {
		c.hub.Remove(c)
		_ = c.connection.Close()
	}()

	for {
		_, rawMsg, err := c.connection.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
			) {
				log.Printf("Client %s unexpected close: %v", c.ID, err)
			}
			return
		}

		var msg messages.ClientMessage
		err = json.Unmarshal(rawMsg, &msg)
		if err != nil {
			log.Printf("JSON Unmarshal Error for Client: %v Message: %s", c.ID, msg)
			c.sendErrorResponse()
			continue
		}

		switch msg.Type {
		case "subscribe":
			c.hub.Subscribe(msg.Symbol, c)
		case "unsubscribe":
			c.hub.Unsubscribe(msg.Symbol, c)
		case "snapshot":
			c.sendSnapshot(msg.Symbol)
		}

		log.Printf("Client: %v Received Message from Client: %s", c.ID, msg)
	}
}

func (c *Client) writeLoop() {
	defer func(connection *websocket.Conn) {
		_ = connection.Close()
	}(c.connection)

	for msg := range c.send {
		err := c.connection.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			return
		}
	}
}

func (c *Client) sendSnapshot(symbol string) {
	orderBook, ok := c.hub.orderBookManager.Get(orderbook.Identifier{
		Exchange: c.hub.exchange,
		Symbol:   symbol,
	})

	var resp map[string]interface{}

	if !ok {
		resp = map[string]interface{}{
			"message": "Symbol is not supported!",
		}
	}

	if orderBook == nil {
		resp = map[string]interface{}{
			"message": "Snapshot is not available!",
		}
	} else {
		resp = map[string]interface{}{
			"type":   "snapshot",
			"symbol": symbol,
			"bids":   orderBook.Bids,
			"asks":   orderBook.Asks,
		}
	}

	data, _ := json.Marshal(resp)
	c.send <- data

}

func (c *Client) sendErrorResponse() {
	resp := map[string]interface{}{
		"message": "Invalid Request!",
	}
	data, _ := json.Marshal(resp)
	c.send <- data
}
