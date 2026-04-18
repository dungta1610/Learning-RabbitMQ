package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	maxRetry   = 2
	workerTag  = "worker-v10"
	amqpURL    = "amqp://guest:guest@localhost:5672/"
	retryDelay = 3 * time.Second
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func declareTopology(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"main_exchange_v10",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare main exchange")

	err = ch.ExchangeDeclare(
		"retry_exchange_v10",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare retry exchange")

	err = ch.ExchangeDeclare(
		"dlx_v10",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLX")

	mainArgs := amqp.Table{
		"x-dead-letter-exchange":    "dlx_v10",
		"x-dead-letter-routing-key": "failed",
	}

	mainQ, err := ch.QueueDeclare(
		"task_queue_v10",
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
		"main_exchange_v10",
		false,
		nil,
	)
	failOnError(err, "failed to bind main queue")

	retryArgs := amqp.Table{
		"x-message-ttl":             int32(5000),
		"x-dead-letter-exchange":    "main_exchange_v10",
		"x-dead-letter-routing-key": "work",
	}

	retryQ, err := ch.QueueDeclare(
		"retry_queue_v10",
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
		"retry_exchange_v10",
		false,
		nil,
	)
	failOnError(err, "failed to bind retry queue")

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
		"retry_exchange_v10",
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
		return errors.New("publisher confirm not enabled")
	}

	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("publish was nacked by broker")
	}
	return nil
}

func processDelivery(ch *amqp.Channel, workerName string, d amqp.Delivery) {
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
				return
			}

			log.Printf("[%s] retry publish confirmed -> ack original", workerName)
			if err := d.Ack(false); err != nil {
				log.Printf("[%s] ack failed: %v", workerName, err)
			}
			return
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

func runConsumerSession(appCtx context.Context, workerName string) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	declareTopology(ch)

	if err := ch.Confirm(false); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}

	msgs, err := ch.Consume(
		"task_queue_v10",
		workerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	connCloseCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	chCloseCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	cancelCh := ch.NotifyCancel(make(chan string, 1))

	log.Printf("[%s] connected and waiting for messages...", workerName)

	processingDone := make(chan struct{})

	go func() {
		defer close(processingDone)
		for d := range msgs {
			processDelivery(ch, workerName, d)
		}
		log.Printf("[%s] delivery channel closed", workerName)
	}()

	for {
		select {
		case <-appCtx.Done():
			log.Printf("[%s] shutdown signal received -> Channel.Cancel(%q)", workerName, workerTag)
			if err := ch.Cancel(workerTag, false); err != nil {
				log.Printf("[%s] Channel.Cancel failed: %v", workerName, err)
			}
			<-processingDone
			log.Printf("[%s] graceful shutdown completed", workerName)
			return nil

		case tag := <-cancelCh:
			log.Printf("[%s] consumer canceled by server, tag=%s", workerName, tag)
			<-processingDone
			return errors.New("consumer canceled by server")

		case err, ok := <-chCloseCh:
			if !ok || err == nil {
				<-processingDone
				return errors.New("channel closed")
			}
			log.Printf("[%s] channel closed unexpectedly: %v", workerName, err)
			<-processingDone
			return err

		case err, ok := <-connCloseCh:
			if !ok || err == nil {
				<-processingDone
				return errors.New("connection closed")
			}
			log.Printf("[%s] connection closed unexpectedly: %v", workerName, err)
			<-processingDone
			return err
		}
	}
}

func main() {
	workerName := os.Getenv("WORKER_NAME")
	if workerName == "" {
		workerName = "worker-1"
	}

	appCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		if appCtx.Err() != nil {
			log.Printf("[%s] application stopped", workerName)
			return
		}

		err := runConsumerSession(appCtx, workerName)
		if err == nil {
			log.Printf("[%s] session ended gracefully", workerName)
			return
		}

		if appCtx.Err() != nil {
			log.Printf("[%s] application stopped", workerName)
			return
		}

		log.Printf("[%s] will reconnect in %v...", workerName, retryDelay)
		time.Sleep(retryDelay)
	}
}
