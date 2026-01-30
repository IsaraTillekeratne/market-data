package orderbook

import (
	"log"
	"strconv"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
)

type OrderBook struct {
	Bids map[string]string
	Asks map[string]string
}

func (orderBook *OrderBook) ApplyBufferedEvents(buffer []models.DiffBookDepthResponse, localUpdateID int64) (int64, bool) {
	applied := 0

	for _, event := range buffer {
		if event.U == nil || event.Smallu == nil {
			log.Println("WARNING: Event missing U or u field, skipping")
			continue
		}

		eventU := *event.U
		eventSmallu := *event.Smallu

		// Step 7: Skip if event.u < local_update_id
		if eventSmallu < localUpdateID {
			continue
		}

		if eventU > localUpdateID+1 {
			log.Printf("WARNING: Gap detected between events (event.U=%d, local_update_id=%d). Resync required.",
				eventU, localUpdateID)
			return localUpdateID, false
		}

		orderBook.ApplyUpdate(event)
		localUpdateID = eventSmallu
		applied++
	}

	log.Printf("Applied %d buffered events. Local order book now synced to %d.", applied, localUpdateID)
	return localUpdateID, true
}

func (orderBook *OrderBook) ApplyUpdate(event models.DiffBookDepthResponse) {
	for _, bid := range event.B {
		if len(bid) >= 2 {
			price := bid[0]
			qty := bid[1]

			qtyFloat, _ := strconv.ParseFloat(qty, 64)
			if qtyFloat == 0 {
				delete(orderBook.Bids, price)
			} else {
				orderBook.Bids[price] = qty
			}
		}
	}

	for _, ask := range event.A {
		if len(ask) >= 2 {
			price := ask[0]
			qty := ask[1]

			qtyFloat, _ := strconv.ParseFloat(qty, 64)
			if qtyFloat == 0 {
				delete(orderBook.Asks, price)
			} else {
				orderBook.Asks[price] = qty
			}
		}
	}
}
