package snapshot

type DepthSnapshot struct {
	LastUpdateId *int64
	Bids         map[string]string
	Asks         map[string]string
}
