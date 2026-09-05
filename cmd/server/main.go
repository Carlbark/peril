package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Error connecting to RabbitMQ")
		return
	}
	defer conn.Close()
	fmt.Println("Conection to RabbitMQ was successful")

	pauseResumeChan, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating pauseResumeChan: %s:", err)
		return
	}

	msg := routing.PlayingState{
		IsPaused: true,
	}
	err = pubsub.PublishJSON(pauseResumeChan, routing.ExchangePerilDirect, routing.PauseKey, msg)
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down...")

}
