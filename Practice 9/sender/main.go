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
	err := ch.ExchangeDeclare(
		"main_exchange_v9",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare main exchange")

	err = ch.ExchangeDeclare(
		"retry_exchange_v9",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare retry exchange")

	err = ch.ExchangeDeclare(
		"dlx_v9",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLX")

	mainArgs := amqp.Table{
		"x-dead-letter-exchange":    "dlx_v9",
		"x-dead-letter-routing-key": "failed",
	}

	mainQ, err := ch.QueueDeclare(
		"task_queue_v9",
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
		"main_exchange_v9",
		false,
		nil,
	)
	failOnError(err, "failed to bind main queue")

	retryArgs := amqp.Table{
		"x-message-ttl":             int32(5000),
		"x-dead-letter-exchange":    "main_exchange_v9",
		"x-dead-letter-routing-key": "work",
	}

	retryQ, err := ch.QueueDeclare(
		"retry_queue_v9",
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
		"retry_exchange_v9",
		false,
		nil,
	)
	failOnError(err, "failed to bind retry queue")

	dlq, err := ch.QueueDeclare(
		"dead_queue_v9",
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
		"dlx_v9",
		false,
		nil,
	)
	failOnError(err, "failed to bind DLQ")
}

func publishWithConfirm(ch *amqp.Channel, exchange, routingKey string, msg amqp.Publishing) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc, err := ch.PublishWithDeferredConfirm(exchange, routingKey, false, false, msg)
	if err != nil {
		return err
	}
	if dc == nil {
		return amqp.ErrClosed
	}

	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return amqp.ErrClosed
	}

	return nil
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	declareTopology(ch)

	err = ch.Confirm(false)
	failOnError(err, "failed to enable publisher confirms")

	body := bodyFromArgs(os.Args)

	err = publishWithConfirm(
		ch,
		"main_exchange_v9",
		"work",
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		},
	)
	failOnError(err, "failed to publish with confirm")

	log.Printf("[x] Sent and confirmed: %s", body)
}
