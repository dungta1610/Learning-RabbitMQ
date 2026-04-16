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

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	// Declare DLX
	err = ch.ExchangeDeclare(
		"dlx_v7",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLX")

	// Declare DLQ
	q, err := ch.QueueDeclare(
		"dead_queue_v7",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLQ")

	err = ch.QueueBind(
		q.Name,
		"failed",
		"dlx_v7",
		false,
		nil,
	)
	failOnError(err, "failed to bind DLQ")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true, // demo đơn giản: auto-ack
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
