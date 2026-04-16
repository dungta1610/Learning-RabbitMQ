package main

import (
	"context"
	"log"
	"time"

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

	q, err := ch.QueueDeclare(
		"task_queue",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	failOnError(err, "failed to declare queue")

	jobs := []string{
		"job-1.....",
		"job-2.",
		"job-3.....",
		"job-4.",
		"job-5.....",
		"job-6.",
	}

	for _, body := range jobs {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		err = ch.PublishWithContext(
			ctx,
			"",     // default exchange
			q.Name, // routing key = queue name
			false,
			false,
			amqp.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp.Persistent,
				Body:         []byte(body),
			},
		)
		cancel()
		failOnError(err, "failed to publish message")

		log.Printf("[x] Sent: %s", body)
	}
}
