package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func declareTopology(ch *amqp.Channel) {
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

	msgs, err := ch.Consume(
		"dead_queue_v8",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to consume DLQ")

	log.Println("[DLQ] waiting for dead-lettered messages...")

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("[DLQ] RECEIVED body=%q headers=%v", string(d.Body), d.Headers)
		}
	}()

	<-forever
}
