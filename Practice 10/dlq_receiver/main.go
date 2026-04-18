package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const amqpURL = "amqp://guest:guest@localhost:5672/"

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func declareTopology(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"dlx_v10",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLX")

	dlq, err := ch.QueueDeclare(
		"dead_queue_v10",
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
		"dlx_v10",
		false,
		nil,
	)
	failOnError(err, "failed to bind DLQ")
}

func main() {
	conn, err := amqp.Dial(amqpURL)
	failOnError(err, "failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	declareTopology(ch)

	msgs, err := ch.Consume(
		"dead_queue_v10",
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
