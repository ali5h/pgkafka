/*
pgkafka consumes postgres logical replication logs, which are decoded by decoderbufs plugin.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jackc/pgx"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ClientConfig used to create a Client
type ClientConfig struct {
	postgres   string
	slot       string
	startpos   uint64
	brokers    string
	topic      string
	maxPending int
	frequency  int
	keepAlive  int
	debug      bool
}

// Client is the client context
type Client struct {
	conn *pgx.ReplicationConn

	producer sarama.AsyncProducer

	// to read/ack postgres
	messages chan *pgx.WalMessage
	flush    chan *pgx.WalMessage

	logger *zap.SugaredLogger
	config *ClientConfig
}

func isAlive(client *Client) bool {
	return client.conn != nil && client.conn.IsAlive()
}

// newClient creates db connection, kafka producer and context
func newClient(config ClientConfig, logger *zap.SugaredLogger) (client *Client, err error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = "pgkafka"
	kafkaConfig.Version = sarama.V0_10_1_0
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Flush.Frequency = time.Duration(config.frequency) * time.Millisecond
	kafkaConfig.Producer.Flush.MaxMessages = config.maxPending

	kafka, err := sarama.NewClient(strings.Split(config.brokers, ","), kafkaConfig)
	if err != nil {
		return nil, err
	}

	async, err := sarama.NewAsyncProducerFromClient(kafka)
	if err != nil {
		return nil, err
	}

	client = &Client{
		producer: async,

		messages: make(chan *pgx.WalMessage),
		flush:    make(chan *pgx.WalMessage),

		config: &config,
		logger: logger,
	}
	return client, nil
}

// Fetches the restart and flushed LSN of the slot, to establish a starting
// point. This function uses a postgresql 9.6 specific column
func getConfirmedFlushLsnFor(postgres string, slot string) (pos uint64, err error) {
	connConfig, err := pgx.ParseConnectionString(postgres)
	if err != nil {
		return
	}

	conn, err := pgx.Connect(connConfig)
	if err != nil {
		return
	}
	defer conn.Close()
	rows, err := conn.Query(fmt.Sprintf("select confirmed_flush_lsn from pg_replication_slots where slot_name='%s'", slot))
	if err != nil {
		return
	}

	defer rows.Close()

	var lsn string
	for rows.Next() {
		rows.Scan(&lsn)
	}

	return pgx.ParseLSN(lsn)
}

// startReplication starts the logical replication connection
func (client *Client) startReplication() error {
	// TODO: we can create the slot if it does not exist
	// close the connection first to prevent deadlock
	if client.conn != nil {
		client.conn.Close()
	}

	connConfig, err := pgx.ParseConnectionString(client.config.postgres)
	if err != nil {
		return err
	}

	client.conn, err = pgx.ReplicationConnect(connConfig)
	if err != nil {
		return err
	}
	return client.conn.StartReplication(client.config.slot, 0, -1)
}

type replMsg struct {
	msg *pgx.ReplicationMessage
	err error
}

/*
  fetch the next message from postgres
  retries dropped db connection
  - it processes the messages fetched from postgres, there are two types of messages
	- server heartbeat: has the last position of the log on the server, it may also ask for status update
	- WAL messages: will store them in a buffer to send later
  - sends priodic keep alive status updates
*/
func (client *Client) fetch(ctx context.Context) error {
	// appended by fetch; consumed by send
	var pending []*pgx.WalMessage
	// syncs postgres fetch go-routine
	messages := make(chan *replMsg)
	// initialy nil (nothing to do) until we have the connection setup
	var startFetch <-chan time.Time

	// keep alive runs in the background
	keepAlive := time.Tick(time.Duration(client.config.keepAlive) * time.Second)
	// save the last status update position to prevent sending too many updates
	// to postgres
	var pos, flushed, lastPos, lastFlushed uint64
	// collecting some stats
	logger := client.logger
	inFlight := 0

	// a helper function for sending status updates
	sendStandbyStatus := func(forced bool) {
		if !isAlive(client) {
			return
		}

		// do not send the update when only the flush position is changing
		if !forced &&
			lastPos == pos &&
			lastFlushed != flushed {
			return
		}

		status, err := pgx.NewStandbyStatus(flushed, 0, pos)
		if err != nil {
			logger.Error(err)
		}

		err = client.conn.SendStandbyStatus(status)
		if err != nil {
			logger.Error(err)
		}

		lastPos = pos
		lastFlushed = flushed
		logger.Debugw("standby",
			"pos", pgx.FormatLSN(pos),
			"flush", pgx.FormatLSN(flushed),
			"last_pos", pgx.FormatLSN(lastPos),
			"last_flush", pgx.FormatLSN(lastFlushed),
			"forced", forced,
		)
		return
	}

	logStatus := func() {
		if inFlight == 0 {
			return
		}
		logger.Infow("postgres",
			"alive", isAlive(client),
			"pos", pgx.FormatLSN(lastPos),
			"flushed", pgx.FormatLSN(lastFlushed),
			"in_flight", inFlight,
		)
	}

	flushed = client.config.startpos

	if err := client.startReplication(); err != nil {
		return err
	}

	client.logger.Info("Startred streaming")
	for {
		var sink chan *pgx.WalMessage
		var message *pgx.WalMessage

		if len(pending) == 0 {
			if startFetch == nil {
				startFetch = time.After(time.Duration(0))
			}
		} else {
			// we have pending messages, send the first one
			message = pending[0]
			sink = client.messages
		}

		select {
		case <-keepAlive:
			sendStandbyStatus(true)
			logStatus()
		case ack := <-client.flush:
			// new flush position is acked
			inFlight = inFlight - 1
			flushed = ack.WalStart
			sendStandbyStatus(false)
		case <-startFetch:
			// go fetch messages from postgres
			go func() {
				r, err := client.conn.WaitForReplicationMessage(ctx)
				messages <- &replMsg{msg: r, err: err}
			}()
		case sink <- message:
			pending = pending[1:]
			inFlight = inFlight + 1
		case rm := <-messages:
			startFetch = nil
			if rm.err != nil {
				return errors.Wrap(rm.err, "fetch failed")
			}
			r := rm.msg
			if r != nil {
				if r.ServerHeartbeat != nil {
					logger.Debugw("Heatbeat", "wal", r.ServerHeartbeat.String())
					if r.ServerHeartbeat.ServerWalEnd > pos {
						pos = r.ServerHeartbeat.ServerWalEnd
					}
					if r.ServerHeartbeat.ReplyRequested == 1 {
						sendStandbyStatus(true)
					} else if pos > flushed && inFlight == 0 {
						// no pending messages left, we can update flush position and send a
						// status update
						logger.Debug("No pending message")
						flushed = pos
						sendStandbyStatus(false)
						logStatus()
					}
				}
				if r.WalMessage != nil {
					logger.Debugw("Message", "wal", r.WalMessage.String())
					if r.WalMessage.WalStart > pos {
						pos = r.WalMessage.WalStart
					}
					pending = append(pending, r.WalMessage)
				}
			}
		case <-ctx.Done():
			logger.Debug("fetch is done")
			return nil
		}
	}
}

// reads from batched, and sends ack on delivered
// order is not guranteed
func (client *Client) write(ctx context.Context) error {
	var acked []*pgx.WalMessage
	var pending []*pgx.WalMessage
	logger := client.logger
	status := time.Tick(time.Duration(client.config.keepAlive) * time.Second)
	inFlight := 0

	logStatus := func() {
		if inFlight == 0 {
			return
		}
		logger.Infow("kafka",
			"not_sent", len(pending),
			"in_flight", inFlight,
		)
	}

	for {
		var source chan *pgx.WalMessage
		var ackc chan *pgx.WalMessage
		var sink chan<- *sarama.ProducerMessage
		var ack *pgx.WalMessage
		var msg *sarama.ProducerMessage

		if len(acked) > 0 {
			ack = acked[0]
			ackc = client.flush
		}

		if len(pending) > 0 {
			first := pending[0]
			msg = &sarama.ProducerMessage{
				Topic:     client.config.topic,
				Value:     sarama.ByteEncoder(first.WalData),
				Timestamp: time.Now(),
				Metadata:  first, // so we can later check
			}
			sink = client.producer.Input()
		} else {
			source = client.messages
		}

		select {
		case <-status:
			logStatus()
		case ackc <- ack:
			acked = acked[1:]
			inFlight = inFlight - 1
		case m := <-source:
			inFlight = inFlight + 1
			pending = append(pending, m)
		case sink <- msg:
			pending = pending[1:]
		case event := <-client.producer.Successes():
			if m, ok := event.Metadata.(*pgx.WalMessage); ok {
				logger.Debugw("Delivered",
					"wal", pgx.FormatLSN(m.WalStart),
					"event", event,
				)
				acked = append(acked, m)
			}
		case err := <-client.producer.Errors():
			return err.Err
		case <-ctx.Done():
			logger.Debug("write is done")
			return nil
		}
	}
}

// CaptureSig captures os signals so we can stop the process
func CaptureSig(ctx context.Context) error {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sigs:
		return fmt.Errorf("sig: %v", s)
	case <-ctx.Done():
		return fmt.Errorf("ctx canceled")
	}
}

// Parses the flags, and start replication and message handling pipelines
func main() {
	// flags
	// TODO: use toml-based config file
	cfg := ClientConfig{
		postgres:   "postgresql://localhost:5432/postgres",
		slot:       "decoderbufs",
		brokers:    "localhost:9092",
		topic:      "qad",
		maxPending: 0,
		frequency:  5,
		keepAlive:  10,
		debug:      false,
	}
	flag.StringVar(&cfg.postgres, "postgres", cfg.postgres, "Postgres DB URI")
	flag.StringVar(&cfg.slot, "slot", cfg.slot, "Slot name")
	flag.StringVar(&cfg.brokers, "brokers", cfg.brokers, "Kafka broker address")
	flag.StringVar(&cfg.topic, "topic", cfg.topic, "Kafka topic")
	flag.IntVar(&cfg.maxPending, "max_pending", cfg.maxPending, "Maximum number of messages allowed on the producer queue (0 for unlimited)")
	flag.IntVar(&cfg.frequency, "frequncy", cfg.frequency, "Frequency, in milliseconds, for flushing the data in the producer queue")
	flag.IntVar(&cfg.keepAlive, "keep_alive", cfg.keepAlive, "Standby status timeout in seconds")
	flag.BoolVar(&cfg.debug, "debug", cfg.debug, "Enables debug logging")
	flag.Parse()

	var zapLogger *zap.Logger
	var err error

	// in debug mode logs are human readable, o.w. uses josn format
	if cfg.debug {
		zapLogger, err = zap.NewDevelopment()
	} else {
		zapLogger, err = zap.NewProduction()
	}
	if err != nil {
		log.Fatal("No logger")
	}
	logger := zapLogger.Sugar()

	// test if uri can be parsed
	_, err = pgx.ParseConnectionString(cfg.postgres)
	if err != nil {
		logger.Fatal(err)
	}

	client, err := newClient(cfg, logger)

	if err != nil {
		logger.Fatal(errors.Wrap(err, "could not create the client"))
	}

	var g run.Group
	{
		ctx, done := context.WithCancel(context.Background())
		g.Add(func() error {
			return CaptureSig(ctx)
		}, func(err error) {
			done()
		})
	}
	{
		ctx, done := context.WithCancel(context.Background())
		g.Add(func() error {
			return client.fetch(ctx)
		}, func(err error) {
			done()
		})
		g.Add(func() error {
			return client.write(ctx)
		}, func(err error) {
			done()
		})
	}
	if err := g.Run(); err != nil {
		logger.Fatal(err)
	}
}
