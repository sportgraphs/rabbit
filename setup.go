package rabbit

import (
	"github.com/streadway/amqp"
	"fmt"
	"errors"
	"context"
)

type MQ interface {
	GetConsumer(name string) (*Consumer, error)
	SetConsumerHandler(name string, ctx context.Context, handler ConsumerHandler) error
	GetPublisher(name string) (*Publisher, error)
	Errors() <-chan error
	Close()
}

type mq struct {
	config     Config
	client     *Client
	consumers  *consumersRegistry
	publishers *publisherRegistry
}

// New configures AMQP
func New(config Config) (MQ, error) {
	config.normalize()

	mq := &mq{
		config:     config,
		client:     NewClient(URL(config.GetDSN()), Backoff(DefaultBackoff)),
		consumers:  newConsumersRegistry(len(config.Consumers)),
		publishers: newPublishersRegistry(len(config.Producers)),
	}

	if err:= mq.initialSetup(); err != nil {
		return nil, err
	}

	go func() {
		for mq.client.Loop() {
			select {
			case err := <-mq.client.Errors():
				fmt.Println(err)
			}
		}
	}()

	return mq, nil
}


func (mq *mq) SetConsumerHandler(name string, ctx context.Context, handler ConsumerHandler) error {
	consumer, err := mq.GetConsumer(name)
	if err != nil {
		return err
	}

	consumer.Consume(ctx, handler)

	return nil
}

// GetConsumer returns a consumer by its name or error if consumer wasn't found.
func (mq *mq) GetConsumer(name string) (consumer *Consumer, err error) {
	consumer, ok := mq.consumers.Get(name)
	if !ok {
		err = fmt.Errorf("consumer '%s' is not registered", name)
		return nil, err
	}

	return consumer, nil
}

func (mq *mq) Errors() <-chan error {
	return mq.client.Errors()
}

// GetProducer returns a producer by its name or error if producer wasn't found.
func (mq *mq) GetPublisher(name string) (publisher *Publisher, err error) {
	publisher, ok := mq.publishers.Get(name)
	if !ok {
		err = fmt.Errorf("publisher '%s' is not registered", name)
		return nil, err
	}

	return publisher, nil
}

// Shutdown all workers and close connection to the message broker.
func (mq *mq) Close() {
	mq.client.Close()
}

func (mq *mq) initialSetup() error {
	if err := mq.setupExchanges(); err != nil {
		return err
	}

	if err := mq.setupQueues(); err != nil {
		return err
	}

	if err := mq.setupBindings(); err != nil {
		return err
	}

	if err := mq.setupProducers(); err != nil {
		return err
	}

	return mq.setupConsumers()
}

func (mq *mq) setupProducers() error {
	for _, config := range mq.config.Producers {
		if err := mq.registerProducer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) registerProducer(config ProducerConfig) error {
	if _, ok := mq.publishers.Get(config.Name); ok {
		return fmt.Errorf(`publisher with name "%s" is already registered`, config.Name)
	}

	publisher := NewPublisher(config.Exchange, config.RoutingKey)

	mq.client.Publish(publisher)
	mq.publishers.Set(config.Name, publisher)

	return nil
}

func (mq *mq) setupConsumers() error {
	for _, config := range mq.config.Consumers {
		if err := mq.registerConsumer(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) registerConsumer(config ConsumerConfig) error {
	if _, ok := mq.consumers.Get(config.Name); ok {
		return fmt.Errorf(`consumer with name "%s" is already registered`, config.Name)
	}

	// Consumer must have at least one worker.
	if config.Workers == 0 {
		config.Workers = 1
	}
	// TODO: QoS, config

	consumer := NewConsumer(config.Queue) // We need to save a whole config for reconnect.

	mq.client.Consume(consumer)
	mq.consumers.Set(config.Name, consumer) // Workers will start after consumer.Consume method call.

	return nil
}

func (mq *mq) setupExchanges() error {
	for _, config := range mq.config.Exchanges {
		if err := mq.declareExchange(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) declareExchange(config ExchangeConfig) error {
	opt := config.Options

	var (
		durable, autoDelete bool
		args                amqp.Table
	)

	if v, ok := opt["durable"]; ok {
		durable, ok = v.(bool)

		if !ok {
			return errors.New("durable option is of type bool")
		}
	}

	if v, ok := opt["autoDelete"]; ok {
		autoDelete, ok = v.(bool)

		if !ok {
			return errors.New("autoDelete option is of type bool")
		}
	}

	if v, ok := opt["args"]; ok {
		args, ok = v.(amqp.Table)

		if !ok {
			return errors.New("args is of type amqp.Table")
		}
	}

	exchange := Exchange{
		Name:       config.Name,
		Kind:       config.Type,
		AutoDelete: autoDelete,
		Durable:    durable,
		Args:       args,
	}

	mq.client.Declare([]Declaration{DeclareExchange(exchange)})

	return nil
}

func (mq *mq) setupBindings() error {
	for _, config := range mq.config.Bindings {
		if err := mq.declareBinding(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) declareBinding(config BindingConfig) error {
	opt := config.Options

	var (
		args amqp.Table
	)

	if v, ok := opt["args"]; ok {
		args, ok = v.(amqp.Table)

		if !ok {
			return errors.New("args is of type amqp.Table")
		}
	}

	exchangeBind := ExchangeBinding{config.Destination, config.RoutingKey, config.Source, args}

	mq.client.Declare([]Declaration{DeclareExchangeBinding(exchangeBind)})

	return nil
}

func (mq *mq) setupQueues() error {
	for _, config := range mq.config.Queues {
		if err := mq.declareQueue(config); err != nil {
			return err
		}
		if err := mq.bindQueue(config); err != nil {
			return err
		}
	}

	return nil
}

func (mq *mq) declareQueue(config QueueConfig) error {
	opt := config.Options

	var (
		durable, autoDelete, exclusive bool
		args                           amqp.Table
	)

	if v, ok := opt["durable"]; ok {
		durable, ok = v.(bool)

		if !ok {
			return errors.New("durable option is of type bool")
		}
	}

	if v, ok := opt["autoDelete"]; ok {
		autoDelete, ok = v.(bool)

		if !ok {
			return errors.New("autoDelete option is of type bool")
		}
	}

	if v, ok := opt["exclusive"]; ok {
		exclusive, ok = v.(bool)

		if !ok {
			return errors.New("exclusive option is of type bool")
		}
	}

	if v, ok := opt["args"]; ok {
		args, ok = v.(amqp.Table)

		if !ok {
			return errors.New("args is of type amqp.Table")
		}
	}

	queue := Queue{
		Name:       config.Name,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		Durable:    durable,
		Args:       args,
	}

	mq.client.Declare([]Declaration{DeclareQueue(queue)})

	return nil
}

func (mq *mq) bindQueue(config QueueConfig) error {
	bindingOptions := config.BindingOptions

	var (
		args   amqp.Table
	)

	if v, ok := bindingOptions["args"]; ok {
		args, ok = v.(amqp.Table)

		if !ok {
			return errors.New("args is of type amqp.Table")
		}
	}

	queueBinding := QueueBinding{
		QueueName: config.Name,
		ExchangeName: config.Exchange,
		Key: config.RoutingKey,
		Args: args,
	}

	mq.client.Declare([]Declaration{DeclareQueueBinding(queueBinding)})

	return nil
}
