package ws

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// upgrader is used to change the http to ws connection
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allows request from any origin
	},
}

func New(hub *Hub) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWS(hub, w, r)
	})
	return mux
}

func ServeWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade Error:", err)
		return
	}

	client := NewClient(conn, hub)
	hub.Add(client)

	go client.writeLoop()
	go client.readLoop()
}
