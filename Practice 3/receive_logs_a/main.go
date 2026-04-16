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
		"logs",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // server-generated name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false,
		nil,
	)
	failOnError(err, "failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		"logs",
		false,
		nil,
	)
	failOnError(err, "failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true, // auto-ack OK cho demo logger đơn giản
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to register a consumer")

	log.Println("[A] Waiting for logs...")

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			log.Printf("[A] %s", d.Body)
		}
	}()

	<-forever
}
