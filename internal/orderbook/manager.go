package orderbook

import "sync"

type Manager struct {
	mu         sync.Mutex
	OrderBooks map[Identifier]*OrderBook
}

func NewManager() *Manager {
	return &Manager{
		OrderBooks: make(map[Identifier]*OrderBook),
	}
}

func (m *Manager) Set(ob *OrderBook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.OrderBooks[ob.Identifier] = ob
}

func (m *Manager) Get(id Identifier) (*OrderBook, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ob, ok := m.OrderBooks[id]
	return ob, ok
}

func (m *Manager) GetAll() []*OrderBook {
	m.mu.Lock()
	defer m.mu.Unlock()
	obs := make([]*OrderBook, 0)
	for _, ob := range m.OrderBooks {
		obs = append(obs, ob)
	}
	return obs
}
