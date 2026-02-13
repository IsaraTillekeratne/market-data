package messages

type ClientMessage struct {
	Type   string `json:"type"`
	Symbol string `json:"symbol"`
}
