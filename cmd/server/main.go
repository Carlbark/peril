package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
		fmt.Println("Error creating pauseResumeChan:", err)
		return
	}

	gamelogic.PrintServerHelp()

	for {

		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		} else {
			switch input[0] {
			case "pause":
				fmt.Println("Sending pause message..")
				msg := routing.PlayingState{
					IsPaused: true,
				}
				err = pubsub.PublishJSON(pauseResumeChan, routing.ExchangePerilDirect, routing.PauseKey, msg)
				if err != nil {
					fmt.Println("Error publishing JSON in pauseResumeChan:", err)
					return
				}
			case "resume":
				fmt.Println("Sending resume message..")
				msg := routing.PlayingState{
					IsPaused: false,
				}
				err = pubsub.PublishJSON(pauseResumeChan, routing.ExchangePerilDirect, routing.PauseKey, msg)
				if err != nil {
					fmt.Println("Error publishing JSON in pauseResumeChan:", err)
					return
				}
			case "quit":
				fmt.Println("Closing server..")
				return
			default:
				fmt.Println("Unknown command..")
			}
		}
	}
}
