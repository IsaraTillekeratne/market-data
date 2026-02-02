package binance

import (
	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/market-data/internal/constants"
)

type Binance struct {
	client *client.BinanceSpotClient
}

func New() *Binance {
	return &Binance{
		client: NewSpotClient(),
	}
}

func (binance *Binance) Name() string {
	return string(constants.ExchangeBinance)
}
