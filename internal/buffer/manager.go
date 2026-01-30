package buffer

import (
	"sync"

	"github.com/binance/binance-connector-go/clients/spot/src/websocketstreams/models"
)

type Manager struct {
	buffer     []models.DiffBookDepthResponse
	bufferLock sync.Mutex
	ErrChan    chan error
}

func NewBufferManager() *Manager {
	return &Manager{
		buffer:  make([]models.DiffBookDepthResponse, 0),
		ErrChan: make(chan error, 1),
	}
}

func (bm *Manager) GetBuffer() []models.DiffBookDepthResponse {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()

	result := make([]models.DiffBookDepthResponse, len(bm.buffer))
	copy(result, bm.buffer)
	return result
}

func (bm *Manager) GetFirst() *models.DiffBookDepthResponse {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()

	if len(bm.buffer) == 0 {
		return nil
	}
	return &bm.buffer[0]
}

func (bm *Manager) IsEmpty() bool {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()
	return len(bm.buffer) == 0
}

func (bm *Manager) Len() int {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()
	return len(bm.buffer)
}

func (bm *Manager) Clear() {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()
	bm.buffer = make([]models.DiffBookDepthResponse, 0)
}

func (bm *Manager) Append(message models.DiffBookDepthResponse) {
	bm.bufferLock.Lock()
	bm.buffer = append(bm.buffer, message)
	bm.bufferLock.Unlock()
}

func (bm *Manager) RemoveOldEvents(localUpdateID int64) {
	bm.bufferLock.Lock()
	defer bm.bufferLock.Unlock()

	filtered := make([]models.DiffBookDepthResponse, 0)
	for _, ev := range bm.buffer {
		if ev.Smallu != nil && *ev.Smallu > localUpdateID {
			filtered = append(filtered, ev)
		}
	}
	bm.buffer = filtered
}
