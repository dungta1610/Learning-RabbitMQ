package main

import (
	"bytes"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func main() {
	workerName := os.Getenv("WORKER_NAME")
	if workerName == "" {
		workerName = "worker-unknown"
	}

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

	// Bật dòng này ở phase có prefetch=1
	err = ch.Qos(1, 0, false)
	failOnError(err, "failed to set QoS")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // auto-ack = false
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to register consumer")

	log.Printf("[%s] waiting for messages...", workerName)

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			start := time.Now()
			body := string(d.Body)

			log.Printf("[%s] START job=%s at=%s",
				workerName, body, start.Format("15:04:05"))

			dotCount := bytes.Count(d.Body, []byte("."))
			time.Sleep(time.Duration(dotCount) * time.Second)

			end := time.Now()
			log.Printf("[%s] DONE  job=%s at=%s duration=%v",
				workerName, body, end.Format("15:04:05"), end.Sub(start))

			err := d.Ack(false)
			if err != nil {
				log.Printf("[%s] ack failed: %v", workerName, err)
			} else {
				log.Printf("[%s] ACK   job=%s", workerName, body)
			}
		}
	}()

	<-forever
}
