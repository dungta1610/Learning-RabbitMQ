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

func bodyFromArgs(args []string) string {
	if len(args) < 2 {
		return "ok: default message"
	}
	return strings.Join(args[1:], " ")
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	// Declare main queue so sender can publish safely
	args := amqp.Table{
		"x-dead-letter-exchange":    "dlx_v7",
		"x-dead-letter-routing-key": "failed",
	}

	q, err := ch.QueueDeclare(
		"task_queue_v7",
		true,
		false,
		false,
		false,
		args,
	)
	failOnError(err, "failed to declare main queue")

	body := bodyFromArgs(os.Args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(
		ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		},
	)
	failOnError(err, "failed to publish")

	log.Printf("[x] Sent: %s", body)
}
