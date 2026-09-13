package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Error connecting to rabbitMQ:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection to rabbitMQ successful")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error establishing channel: ", err)
		return
	}
	fmt.Println("Channel to rabbitMQ created")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error getting username:", err)
		return
	}
	gs := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, userName),
		routing.PauseKey, pubsub.SimpleQueueTransient,
		handlerPause(gs))
	if err != nil {
		fmt.Println("Error subscribing: ", err)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), pubsub.SimpleQueueTransient,
		handlerMove(gs, ch))
	if err != nil {
		fmt.Println("Error subscribing: ", err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.SimpleQueueDurable,
		handlerWar(gs))
	if err != nil {
		fmt.Println("Error subscribing: ", err)
		return
	}
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		} else {
			switch input[0] {
			case "spawn":
				fmt.Println("Spawning unit..")
				err := gs.CommandSpawn(input)
				if err != nil {
					fmt.Println(err)
					break
				}
			case "move":
				fmt.Println("Moving unit..")
				am, err := gs.CommandMove(input)
				if err != nil {
					fmt.Println("Error moving units...")
					break
				}
				err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName), am)
				if err != nil {
					fmt.Println("Error publishing move")
					break
				}
				fmt.Println("Unit(s) successfully moved.")
			case "status":
				gs.CommandStatus()
			case "help":
				gamelogic.PrintClientHelp()
			case "spam":
				fmt.Println("Spamming not allowed yet!")
			case "quit":
				gamelogic.PrintQuit()
				return
			default:
				fmt.Println("Unknown command..")
			}
		}

	}

}
