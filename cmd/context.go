package cmd

import "github.com/market-data/internal/app"

var appInstance *app.App

func initApp(cfg app.Config) *app.App {
	if appInstance != nil {
		return appInstance
	}
	appInstance = app.New(cfg)
	return appInstance
}
