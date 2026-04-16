package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func routingKeyFromArgs(args []string) string {
	if len(args) < 2 {
		return "anonymous.info"
	}
	return args[1]
}

func bodyFromArgs(args []string) string {
	if len(args) < 3 {
		return "hello topic exchange"
	}
	return strings.Join(args[2:], " ")
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"topic_logs",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare exchange")

	routingKey := routingKeyFromArgs(os.Args)
	body := bodyFromArgs(os.Args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(
		ctx,
		"topic_logs",
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	failOnError(err, "failed to publish message")

	log.Printf("[x] Sent [%s] %s", routingKey, body)
}
