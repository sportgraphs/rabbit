package rabbit

import "github.com/streadway/amqp"

type Declaration func(Declarer) error

// Declarer is implemented by *amqp.Channel
type Declarer interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
}

func DeclareQueue(q Queue) Declaration {
	name := q.Name
	return func(c Declarer) error {
		q.Name = name
		realQ, err := c.QueueDeclare(q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false,
			q.Args,
		)
		q.l.Lock()
		q.Name = realQ.Name
		q.l.Unlock()
		return err
	}
}

func DeclareExchange(e Exchange) Declaration {
	return func(c Declarer) error {
		return c.ExchangeDeclare(e.Name,
			e.Kind,
			e.Durable,
			e.AutoDelete,
			false,
			false,
			e.Args,
		)
	}
}

// DeclareQueueBinding is a way to declare AMQP binding between AMQP queue and exchange
func DeclareQueueBinding(b QueueBinding) Declaration {
	return func(c Declarer) error {
		return c.QueueBind(b.QueueName,
			b.Key,
			b.ExchangeName,
			false,
			b.Args,
		)
	}
}

func DeclareExchangeBinding(eb ExchangeBinding) Declaration {
	return func (c Declarer) error {
		return c.ExchangeBind(eb.Destination, eb.Key, eb.Source, false, eb.Args)
	}
}
