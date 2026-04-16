package main

import (
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

func main() {
	workerName := os.Getenv("WORKER_NAME")
	if workerName == "" {
		workerName = "worker-1"
	}

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "failed to connect")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "failed to open channel")
	defer ch.Close()

	// 1) Declare DLX
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

	// 2) Declare main queue with dead-letter settings
	args := amqp.Table{
		"x-dead-letter-exchange":    "dlx_v7",
		"x-dead-letter-routing-key": "failed",
	}

	q, err := ch.QueueDeclare(
		"task_queue_v7",
		true,
		false,
		false,
		false,
		args,
	)
	failOnError(err, "failed to declare main queue")

	err = ch.Qos(1, 0, false)
	failOnError(err, "failed to set QoS")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to consume")

	log.Printf("[%s] waiting for messages...", workerName)

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			body := string(d.Body)

			log.Printf("[%s] RECEIVED body=%q redelivered=%v",
				workerName, body, d.Redelivered)

			time.Sleep(1 * time.Second)

			switch {
			case strings.HasPrefix(body, "ok:"):
				log.Printf("[%s] OK -> ack", workerName)
				if err := d.Ack(false); err != nil {
					log.Printf("[%s] ack failed: %v", workerName, err)
				}

			case strings.HasPrefix(body, "retry:"):
				if !d.Redelivered {
					log.Printf("[%s] TRANSIENT ERROR -> nack requeue=true", workerName)
					if err := d.Nack(false, true); err != nil {
						log.Printf("[%s] nack(requeue=true) failed: %v", workerName, err)
					}
					continue
				}

				log.Printf("[%s] REDLIVERED -> now treat as success, ack", workerName)
				if err := d.Ack(false); err != nil {
					log.Printf("[%s] ack failed: %v", workerName, err)
				}

			case strings.HasPrefix(body, "bad:"):
				log.Printf("[%s] PERMANENT ERROR -> nack requeue=false (go to DLQ)", workerName)
				if err := d.Nack(false, false); err != nil {
					log.Printf("[%s] nack(requeue=false) failed: %v", workerName, err)
				}

			default:
				log.Printf("[%s] UNKNOWN TYPE -> ack", workerName)
				if err := d.Ack(false); err != nil {
					log.Printf("[%s] ack failed: %v", workerName, err)
				}
			}
		}
	}()

	<-forever
}
