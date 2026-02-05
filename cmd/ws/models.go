package ws

type SnapshotRequest struct {
	Type   string `json:"type"`
	Symbol string `json:"symbol"`
}

type SnapshotResponse struct {
	Type   string            `json:"type"`
	Symbol string            `json:"symbol"`
	Bids   map[string]string `json:"bids"`
	Asks   map[string]string `json:"asks"`
	Error  string            `json:"error,omitempty"`
}
