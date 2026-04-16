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

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	failOnError(err, "failed to declare queue")

	err = ch.QueueBind(
		q.Name,
		"payment.failed.#",
		"topic_logs",
		false,
		nil,
	)
	failOnError(err, "failed to bind queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to register consumer")

	log.Println("[PAYMENT_FAIL] Waiting for payment.failed.# messages...")

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("[PAYMENT_FAIL] %s", d.Body)
		}
	}()

	<-forever
}
