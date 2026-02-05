package cmd

import (
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/market-data/cmd/ws"
	"github.com/spf13/cobra"
)

var symbol string
var wsAddr string
var limit int

type level struct {
	price float64
	qty   string
}

var orderBookCmd = &cobra.Command{
	Use:   "orderbook",
	Short: "Order book operations",
}

var orderBookShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show order book for a symbol",
	Run: func(cmd *cobra.Command, args []string) {
		resp, err := ws.FetchSnapshot(wsAddr, symbol)
		if err != nil {
			log.Fatalf("failed to fetch snapshot: %v", err)
		}

		bids := mapToSortedBids(resp.Bids)
		asks := mapToSortedAsks(resp.Asks)

		fmt.Printf("\nTop %v Bids:\n", limit)
		for i := 0; i < min(limit, len(bids)); i++ {
			fmt.Printf("  %s @ %.2f\n", bids[i].qty, bids[i].price)
		}

		fmt.Printf("\nTop %v Asks:\n", limit)
		for i := 0; i < min(limit, len(asks)); i++ {
			fmt.Printf("  %s @ %.2f\n", asks[i].qty, asks[i].price)
		}

	},
}

func init() {
	orderBookShowCmd.Flags().StringVar(&symbol, "symbol", "", "Trading symbol")
	orderBookShowCmd.Flags().IntVar(&limit, "limit", 5, "Order Book Length")
	orderBookShowCmd.Flags().StringVar(
		&wsAddr,
		"ws",
		"localhost:8080",
		"WebSocket server address",
	)

	_ = orderBookShowCmd.MarkFlagRequired("symbol")

	orderBookCmd.AddCommand(orderBookShowCmd)
	rootCmd.AddCommand(orderBookCmd)
}

func mapToSortedBids(m map[string]string) []level {
	levels := make([]level, 0, len(m))
	for p, q := range m {
		price, _ := strconv.ParseFloat(p, 64)
		levels = append(levels, level{price: price, qty: q})
	}

	sort.Slice(levels, func(i, j int) bool {
		return levels[i].price > levels[j].price // DESC
	})

	return levels
}

func mapToSortedAsks(m map[string]string) []level {
	levels := make([]level, 0, len(m))
	for p, q := range m {
		price, _ := strconv.ParseFloat(p, 64)
		levels = append(levels, level{price: price, qty: q})
	}

	sort.Slice(levels, func(i, j int) bool {
		return levels[i].price < levels[j].price // ASC
	})

	return levels
}
