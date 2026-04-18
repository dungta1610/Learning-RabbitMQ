package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	amqpURL               = "amqp://guest:guest@localhost:5672/"
	workerTag             = "worker-v11"
	reconnectDelay        = 3 * time.Second
	stateFilePath         = "state_v11.json"
	successAfterRetryOnce = 1
	maxRetryFail          = 2
)

type TransferRecord struct {
	EventID  string `json:"event_id"`
	Body     string `json:"body"`
	StoredAt string `json:"stored_at"`
}

type AppState struct {
	Processed map[string]string `json:"processed"`
	Transfers []TransferRecord  `json:"transfers"`
}

type StateStore struct {
	mu    sync.Mutex
	path  string
	state AppState
}

func NewStateStore(path string) (*StateStore, error) {
	s := &StateStore{
		path: path,
		state: AppState{
			Processed: map[string]string{},
			Transfers: []TransferRecord{},
		},
	}

	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StateStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var st AppState
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	if st.Processed == nil {
		st.Processed = map[string]string{}
	}
	if st.Transfers == nil {
		st.Transfers = []TransferRecord{}
	}

	s.state = st
	return nil
}

func (s *StateStore) saveLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *StateStore) HasProcessed(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.state.Processed[eventID]
	return ok
}

func (s *StateStore) ApplyTransferOnce(eventID, body string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.state.Processed[eventID]; ok {
		return false, nil
	}

	s.state.Transfers = append(s.state.Transfers, TransferRecord{
		EventID:  eventID,
		Body:     body,
		StoredAt: time.Now().Format(time.RFC3339),
	})
	s.state.Processed[eventID] = time.Now().Format(time.RFC3339)

	if err := s.saveLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func declareTopology(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"main_exchange_v11",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare main exchange")

	err = ch.ExchangeDeclare(
		"retry_exchange_v11",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare retry exchange")

	err = ch.ExchangeDeclare(
		"dlx_v11",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare DLX")

	mainArgs := amqp.Table{
		"x-dead-letter-exchange":    "dlx_v11",
		"x-dead-letter-routing-key": "failed",
	}

	mainQ, err := ch.QueueDeclare(
		"task_queue_v11",
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
		"main_exchange_v11",
		false,
		nil,
	)
	failOnError(err, "failed to bind main queue")

	retryArgs := amqp.Table{
		"x-message-ttl":             int32(5000),
		"x-dead-letter-exchange":    "main_exchange_v11",
		"x-dead-letter-routing-key": "work",
	}

	retryQ, err := ch.QueueDeclare(
		"retry_queue_v11",
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
		"retry_exchange_v11",
		false,
		nil,
	)
	failOnError(err, "failed to bind retry queue")

	dlq, err := ch.QueueDeclare(
		"dead_queue_v11",
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
		"dlx_v11",
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

func getEventID(headers amqp.Table) (string, error) {
	if headers == nil {
		return "", errors.New("missing headers")
	}

	v, ok := headers["x-event-id"]
	if !ok {
		return "", errors.New("missing x-event-id")
	}

	switch id := v.(type) {
	case string:
		if id == "" {
			return "", errors.New("empty x-event-id")
		}
		return id, nil
	case []byte:
		if len(id) == 0 {
			return "", errors.New("empty x-event-id")
		}
		return string(id), nil
	default:
		return "", errors.New("invalid x-event-id type")
	}
}

func publishRetryWithConfirm(ch *amqp.Channel, eventID string, body []byte, retryCount int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc, err := ch.PublishWithDeferredConfirm(
		"retry_exchange_v11",
		"retry",
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Headers: amqp.Table{
				"x-event-id":    eventID,
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

func processDelivery(ch *amqp.Channel, store *StateStore, workerName string, d amqp.Delivery) {
	body := string(d.Body)
	retryCount := getRetryCount(d.Headers)

	eventID, err := getEventID(d.Headers)
	if err != nil {
		log.Printf("[%s] INVALID MESSAGE -> missing event id: %v", workerName, err)
		if err := d.Nack(false, false); err != nil {
			log.Printf("[%s] nack(requeue=false) failed: %v", workerName, err)
		}
		return
	}

	log.Printf("[%s] RECEIVED eventID=%s body=%q retryCount=%d redelivered=%v",
		workerName, eventID, body, retryCount, d.Redelivered)

	if store.HasProcessed(eventID) {
		log.Printf("[%s] DUPLICATE EVENT -> ack without side effect, eventID=%s", workerName, eventID)
		if err := d.Ack(false); err != nil {
			log.Printf("[%s] ack failed: %v", workerName, err)
		}
		return
	}

	time.Sleep(1 * time.Second)

	switch {
	case strings.HasPrefix(body, "ok:"):
		applied, err := store.ApplyTransferOnce(eventID, body)
		if err != nil {
			log.Printf("[%s] state store error -> nack requeue=true: %v", workerName, err)
			if err := d.Nack(false, true); err != nil {
				log.Printf("[%s] nack(requeue=true) failed: %v", workerName, err)
			}
			return
		}

		if applied {
			log.Printf("[%s] SIDE EFFECT APPLIED ONCE -> ack eventID=%s", workerName, eventID)
		} else {
			log.Printf("[%s] DUPLICATE DETECTED DURING APPLY -> ack eventID=%s", workerName, eventID)
		}
		if err := d.Ack(false); err != nil {
			log.Printf("[%s] ack failed: %v", workerName, err)
		}

	case strings.HasPrefix(body, "retry:"):
		if retryCount < successAfterRetryOnce {
			nextRetry := retryCount + 1
			log.Printf("[%s] TRANSIENT ERROR -> publish to retry queue with confirm, eventID=%s retryCount=%d", workerName, eventID, nextRetry)

			err := publishRetryWithConfirm(ch, eventID, d.Body, nextRetry)
			if err != nil {
				log.Printf("[%s] retry publish confirm failed: %v", workerName, err)
				log.Printf("[%s] fallback -> nack requeue=true to avoid losing original", workerName)
				if err := d.Nack(false, true); err != nil {
					log.Printf("[%s] fallback nack(requeue=true) failed: %v", workerName, err)
				}
				return
			}

			log.Printf("[%s] retry publish confirmed -> ack original eventID=%s", workerName, eventID)
			if err := d.Ack(false); err != nil {
				log.Printf("[%s] ack failed: %v", workerName, err)
			}
			return
		}

		applied, err := store.ApplyTransferOnce(eventID, body)
		if err != nil {
			log.Printf("[%s] state store error after retry -> nack requeue=true: %v", workerName, err)
			if err := d.Nack(false, true); err != nil {
				log.Printf("[%s] nack(requeue=true) failed: %v", workerName, err)
			}
			return
		}

		if applied {
			log.Printf("[%s] RETRY SUCCEEDED, SIDE EFFECT APPLIED ONCE -> ack eventID=%s", workerName, eventID)
		} else {
			log.Printf("[%s] DUPLICATE AFTER RETRY -> ack eventID=%s", workerName, eventID)
		}
		if err := d.Ack(false); err != nil {
			log.Printf("[%s] ack failed: %v", workerName, err)
		}

	case strings.HasPrefix(body, "retryfail:"):
		if retryCount < maxRetryFail {
			nextRetry := retryCount + 1
			log.Printf("[%s] TRANSIENT ERROR (WILL EVENTUALLY FAIL) -> publish retry eventID=%s retryCount=%d", workerName, eventID, nextRetry)

			err := publishRetryWithConfirm(ch, eventID, d.Body, nextRetry)
			if err != nil {
				log.Printf("[%s] retry publish confirm failed: %v", workerName, err)
				log.Printf("[%s] fallback -> nack requeue=true", workerName)
				if err := d.Nack(false, true); err != nil {
					log.Printf("[%s] nack(requeue=true) failed: %v", workerName, err)
				}
				return
			}

			log.Printf("[%s] retry publish confirmed -> ack original eventID=%s", workerName, eventID)
			if err := d.Ack(false); err != nil {
				log.Printf("[%s] ack failed: %v", workerName, err)
			}
			return
		}

		log.Printf("[%s] RETRY LIMIT EXCEEDED -> nack requeue=false (go to DLQ), eventID=%s", workerName, eventID)
		if err := d.Nack(false, false); err != nil {
			log.Printf("[%s] nack(requeue=false) failed: %v", workerName, err)
		}

	case strings.HasPrefix(body, "bad:"):
		log.Printf("[%s] PERMANENT ERROR -> nack requeue=false (go to DLQ), eventID=%s", workerName, eventID)
		if err := d.Nack(false, false); err != nil {
			log.Printf("[%s] nack(requeue=false) failed: %v", workerName, err)
		}

	default:
		applied, err := store.ApplyTransferOnce(eventID, body)
		if err != nil {
			log.Printf("[%s] store error on unknown type -> nack requeue=true: %v", workerName, err)
			if err := d.Nack(false, true); err != nil {
				log.Printf("[%s] nack(requeue=true) failed: %v", workerName, err)
			}
			return
		}

		if applied {
			log.Printf("[%s] UNKNOWN TYPE BUT APPLIED ONCE -> ack eventID=%s", workerName, eventID)
		}
		if err := d.Ack(false); err != nil {
			log.Printf("[%s] ack failed: %v", workerName, err)
		}
	}
}

func runConsumerSession(appCtx context.Context, store *StateStore, workerName string) error {
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
		"task_queue_v11",
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
			processDelivery(ch, store, workerName, d)
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

	store, err := NewStateStore(stateFilePath)
	failOnError(err, "failed to create state store")

	log.Printf("[%s] idempotency store loaded from %s", workerName, stateFilePath)

	appCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		if appCtx.Err() != nil {
			log.Printf("[%s] application stopped", workerName)
			return
		}

		err := runConsumerSession(appCtx, store, workerName)
		if err == nil {
			log.Printf("[%s] session ended gracefully", workerName)
			return
		}

		if appCtx.Err() != nil {
			log.Printf("[%s] application stopped", workerName)
			return
		}

		log.Printf("[%s] will reconnect in %v...", workerName, reconnectDelay)
		time.Sleep(reconnectDelay)
	}
}
