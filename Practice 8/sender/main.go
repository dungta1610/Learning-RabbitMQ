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

func declareTopology(ch *amqp.Channel) {
	// Main exchange
	err := ch.ExchangeDeclare(
		"main_exchange_v8",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare main exchange")

	// Retry exchange
	err = ch.ExchangeDeclare(
		"retry_exchange_v8",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare retry exchange")

	// DLX
	err = ch.ExchangeDeclare(
		"dlx_v8",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLX")

	// Main queue: permanent failures go to DLQ
	mainArgs := amqp.Table{
		"x-dead-letter-exchange":    "dlx_v8",
		"x-dead-letter-routing-key": "failed",
	}

	mainQ, err := ch.QueueDeclare(
		"task_queue_v8",
		true,
		false,
		false,
		false,
		mainArgs,
	)
	failOnError(err, "failed to declare main queue")

	err = ch.QueueBind(
		mainQ.Name,
		"work",
		"main_exchange_v8",
		false,
		nil,
	)
	failOnError(err, "failed to bind main queue")

	// Retry queue: wait 5 seconds, then go back to main queue
	retryArgs := amqp.Table{
		"x-message-ttl":             int32(5000),
		"x-dead-letter-exchange":    "main_exchange_v8",
		"x-dead-letter-routing-key": "work",
	}

	retryQ, err := ch.QueueDeclare(
		"retry_queue_v8",
		true,
		false,
		false,
		false,
		retryArgs,
	)
	failOnError(err, "failed to declare retry queue")

	err = ch.QueueBind(
		retryQ.Name,
		"retry",
		"retry_exchange_v8",
		false,
		nil,
	)
	failOnError(err, "failed to bind retry queue")

	// DLQ
	dlq, err := ch.QueueDeclare(
		"dead_queue_v8",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLQ")

	err = ch.QueueBind(
		dlq.Name,
		"failed",
		"dlx_v8",
		false,
		nil,
	)
	failOnError(err, "failed to bind DLQ")
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	declareTopology(ch)

	body := bodyFromArgs(os.Args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(
		ctx,
		"main_exchange_v8",
		"work",
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
