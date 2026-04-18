package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxRetry = 2

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
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

func getRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}

	v, ok := headers["x-retry-count"]
	if !ok {
		return 0
	}

	switch n := v.(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	default:
		return 0
	}
}

func publishRetryWithConfirm(ch *amqp.Channel, body []byte, retryCount int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc, err := ch.PublishWithDeferredConfirm(
		"retry_exchange_v9",
		"retry",
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Headers: amqp.Table{
				"x-retry-count": int32(retryCount),
			},
			Body: body,
		},
	)
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

	declareTopology(ch)

	err = ch.Confirm(false)
	failOnError(err, "failed to enable publisher confirms")

	err = ch.Qos(1, 0, false)
	failOnError(err, "failed to set QoS")

	msgs, err := ch.Consume(
		"task_queue_v9",
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
			retryCount := getRetryCount(d.Headers)

			log.Printf("[%s] RECEIVED body=%q retryCount=%d redelivered=%v",
				workerName, body, retryCount, d.Redelivered)

			time.Sleep(1 * time.Second)

			switch {
			case strings.HasPrefix(body, "ok:"):
				log.Printf("[%s] OK -> ack", workerName)
				if err := d.Ack(false); err != nil {
					log.Printf("[%s] ack failed: %v", workerName, err)
				}

			case strings.HasPrefix(body, "retry:"):
				if retryCount < maxRetry {
					nextRetry := retryCount + 1
					log.Printf("[%s] TRANSIENT ERROR -> publish to retry queue with confirm, retryCount=%d", workerName, nextRetry)

					err := publishRetryWithConfirm(ch, d.Body, nextRetry)
					if err != nil {
						log.Printf("[%s] retry publish confirm failed: %v", workerName, err)
						log.Printf("[%s] fallback -> nack requeue=true to avoid losing original", workerName)

						if err := d.Nack(false, true); err != nil {
							log.Printf("[%s] fallback nack(requeue=true) failed: %v", workerName, err)
						}
						continue
					}

					log.Printf("[%s] retry publish confirmed -> ack original", workerName)
					if err := d.Ack(false); err != nil {
						log.Printf("[%s] ack failed: %v", workerName, err)
					}
					continue
				}

				log.Printf("[%s] RETRY LIMIT EXCEEDED -> nack requeue=false (go to DLQ)", workerName)
				if err := d.Nack(false, false); err != nil {
					log.Printf("[%s] nack(requeue=false) failed: %v", workerName, err)
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
