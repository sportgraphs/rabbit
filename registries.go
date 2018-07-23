package rabbit

import "sync"

func newPublishersRegistry(size int) *publisherRegistry {
	return &publisherRegistry{
		publishers: make(map[string]*Publisher, size),
	}
}

type publisherRegistry struct {
	sync.Mutex
	publishers map[string]*Publisher
}

func (registry *publisherRegistry) Get(name string) (*Publisher, bool) {
	registry.Lock()
	producer, ok := registry.publishers[name]
	registry.Unlock()

	return producer, ok
}

func (registry *publisherRegistry) Set(name string, publisher *Publisher) {
	registry.Lock()
	registry.publishers[name] = publisher
	registry.Unlock()
}

func (registry *publisherRegistry) GoEach(fn func(*Publisher)) {
	wg := &sync.WaitGroup{}

	registry.Lock()
	defer registry.Unlock()

	wg.Add(len(registry.publishers))

	for _, p := range registry.publishers {
		go func(producer *Publisher) {
			fn(producer)
			wg.Done()
		}(p)
	}

	wg.Wait()
}

func newConsumersRegistry(size int) *consumersRegistry {
	return &consumersRegistry{
		consumers: make(map[string]*Consumer, size),
	}
}

type consumersRegistry struct {
	sync.Mutex
	consumers map[string]*Consumer
}

func (registry *consumersRegistry) Get(name string) (*Consumer, bool) {
	registry.Lock()
	consumer, ok := registry.consumers[name]
	registry.Unlock()

	return consumer, ok
}

func (registry *consumersRegistry) Set(name string, consumer *Consumer) {
	registry.Lock()
	registry.consumers[name] = consumer
	registry.Unlock()
}

func (registry *consumersRegistry) GoEach(fn func(*Consumer)) {
	wg := &sync.WaitGroup{}

	registry.Lock()
	defer registry.Unlock()

	wg.Add(len(registry.consumers))

	for _, c := range registry.consumers {
		go func(consumer *Consumer) {
			fn(consumer)
			wg.Done()
		}(c)
	}

	wg.Wait()
}
