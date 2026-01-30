package binance

import (
	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/binance/binance-connector-go/common/common"
)

func NewSpotClient() *client.BinanceSpotClient {
	return client.NewBinanceSpotClient(
		client.WithRestAPI(common.NewConfigurationRestAPI()),
		client.WithWebsocketStreams(
			common.NewConfigurationWebsocketStreams(),
		),
	)
}
