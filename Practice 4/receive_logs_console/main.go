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
	failOnError(err, "failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"direct_logs",
		"direct",
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

	severities := []string{"info", "warning", "error"}

	for _, s := range severities {
		err = ch.QueueBind(
			q.Name,
			s,
			"direct_logs",
			false,
			nil,
		)
		failOnError(err, "failed to bind queue")
	}

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

	log.Println("[CONSOLE] Waiting for all logs...")

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("[CONSOLE] %s", d.Body)
		}
	}()

	<-forever
}
