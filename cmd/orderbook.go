package cmd

import (
	"fmt"
	"log"

	"github.com/market-data/internal/constants"
	"github.com/market-data/internal/orderbook"
	"github.com/spf13/cobra"
)

var symbol string

var orderBookCmd = &cobra.Command{
	Use:   "orderbook",
	Short: "Order book operations",
}

var orderBookShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show order book for a symbol",
	Run: func(cmd *cobra.Command, args []string) {
		app := getApp()

		// can't read the in memory order book in below approach
		// TODO: instead should call the ws server with a ws request
		ob, ok := app.OrderBookManager.Get(orderbook.Identifier{
			Exchange: string(constants.ExchangeBinance),
			Symbol:   symbol,
		})
		if !ok {
			log.Fatalf("orderbook not found for symbol %s", symbol)
		}

		fmt.Printf("OrderBook %s\n", symbol)
		fmt.Printf("Bids: %d\n", len(ob.Bids))
		fmt.Printf("Asks: %d\n", len(ob.Asks))
	},
}

func init() {
	orderBookShowCmd.Flags().StringVar(&symbol, "symbol", "", "Trading symbol")
	err := orderBookShowCmd.MarkFlagRequired("symbol")
	if err != nil {
		return
	}

	orderBookCmd.AddCommand(orderBookShowCmd)
	rootCmd.AddCommand(orderBookCmd)
}
