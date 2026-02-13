package ws

import (
	"errors"
	"net/url"

	"github.com/gorilla/websocket"
)

func FetchSnapshot(wsURL, symbol string) (*SnapshotResponse, error) {
	u := url.URL{Scheme: "ws", Host: wsURL, Path: "/ws"}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	defer func(conn *websocket.Conn) {
		_ = conn.Close()
	}(conn)

	req := SnapshotRequest{
		Type:   "snapshot",
		Symbol: symbol,
	}

	if err := conn.WriteJSON(req); err != nil {
		return nil, err
	}

	var resp SnapshotResponse
	if err := conn.ReadJSON(&resp); err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		return nil, errors.New(resp.Error)
	}

	return &resp, nil
}
