package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %v", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello2",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("Hello RabbitMQ from Go number %d", i)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		err = ch.PublishWithContext(
			ctx,
			"",
			q.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		cancel()

		failOnError(err, "Failed to publish a message")
		log.Printf("[x] Sent: %s", body)
	}
}
