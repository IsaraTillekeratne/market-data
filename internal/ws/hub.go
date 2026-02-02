package ws

import "sync"

type Hub struct {
	hubLock sync.Mutex
	clients map[*Client]struct{} // struct{} is used since, no need to store the value
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[*Client]struct{}),
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
