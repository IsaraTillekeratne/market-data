package ws

import (
	"log"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	ID         string
	connection *websocket.Conn
	send       chan []byte
	hub        *Hub
}

func NewClient(connection *websocket.Conn, hub *Hub) *Client {
	return &Client{
		ID:         uuid.New().String(),
		connection: connection,
		send:       make(chan []byte, 256),
		hub:        hub,
	}
}

func (c *Client) readLoop() {
	defer func() {
		c.hub.Remove(c)
		_ = c.connection.Close()
	}()

	for {
		_, msg, err := c.connection.ReadMessage()
		if err != nil {
			log.Println("Read Error:", err)
			return
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
