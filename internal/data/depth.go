package data

type DepthUpdate struct {
	Exchange string
	Symbol   string
	Bids     [][]string
	Asks     [][]string
}
