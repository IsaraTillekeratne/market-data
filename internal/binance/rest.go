package binance

import (
	"context"

	client "github.com/binance/binance-connector-go/clients/spot"
	"github.com/market-data/internal/snapshot"
)

func GetDepthSnapshot(client *client.BinanceSpotClient, symbol string, limit int32) (snapshot.DepthSnapshot, error) {
	resp, err := client.RestApi.MarketAPI.Depth(context.Background()).
		Symbol(symbol).
		Limit(limit).
		Execute()

	if err != nil {
		return snapshot.DepthSnapshot{}, err
	}

	bids := make(map[string]string)
	for _, bid := range resp.Data.Bids {
		price := bid[0]
		qty := bid[1]
		bids[price] = qty
	}

	asks := make(map[string]string)
	for _, ask := range resp.Data.Asks {
		price := ask[0]
		qty := ask[1]
		asks[price] = qty
	}

	response := snapshot.DepthSnapshot{
		LastUpdateId: resp.Data.LastUpdateId,
		Bids:         bids,
		Asks:         asks,
	}

	return response, nil
}
