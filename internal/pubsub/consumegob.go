package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeGob[T any](
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
	err = ch.Qos(10, 0, false)
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
			dec := gob.NewDecoder(bytes.NewReader(delivery.Body))

			if err := dec.Decode(&target); err != nil {
				fmt.Printf("could not decode: %v\n", err)
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
