package cmd

import (
	"log"

	"github.com/market-data/internal/app"
	"github.com/market-data/internal/constants"
	"github.com/spf13/cobra"
)

var (
	symbols []string
	port    string
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the market data service",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := app.Config{
			Symbols: symbols,
			Port:    port,
		}
		a := initApp(cfg)
		if err := a.Start(); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	startCmd.Flags().StringSliceVar(
		&symbols,
		"symbols",
		[]string{string(constants.BNBUSDT), string(constants.ETHUSDT)},
		"Symbols to track (comma separated)",
	)

	startCmd.Flags().StringVar(
		&port,
		"port",
		getEnv("PORT", "8080"),
		"WebSocket server port",
	)

	rootCmd.AddCommand(startCmd)
}
