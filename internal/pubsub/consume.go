package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {

	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		defer ch.Close()
		for delivery := range deliveries {
			var target T
			if err := json.Unmarshal(delivery.Body, &target); err != nil {
				fmt.Printf("could not unmarshal: %v\n", err)
				continue
			}
			ackType := handler(target)
			switch ackType {
			case Ack:
				err := delivery.Ack(false)
				if err != nil {
					fmt.Printf("could not ack delivery: %v\n", err)
				}
				fmt.Println("Ack")
			case NackRequeue:
				err := delivery.Nack(false, true)
				if err != nil {
					fmt.Printf("could not requeue delivery: %v\n", err)
				}
				fmt.Println("Nack requeue")
			case NackDiscard:
				err := delivery.Nack(false, false)
				if err != nil {
					fmt.Printf("could not discard delivery: %v\n", err)
				}
				fmt.Println("Nack discard")
			}
		}
	}()
	return nil
}
