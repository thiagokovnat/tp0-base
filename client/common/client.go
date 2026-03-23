package common

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopPeriod     time.Duration
	BatchMaxAmount int
	DataFilePath   string
}

// Client Entity that encapsulates how
type Client struct {
	config  ClientConfig
	conn    net.Conn
	running bool
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:  config,
		running: true,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

func chunkRows(rows [][6]string, batchSize int) [][][6]string {
	if batchSize < 1 {
		batchSize = 1
	}
	var chunks [][][6]string
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunks = append(chunks, rows[i:end])
	}
	return chunks
}

func (c *Client) notifyFinished() error {
	if err := c.createClientSocket(); err != nil {
		return err
	}
	proto := NewBetProtocol(c.conn)
	err := proto.SendFinishedNotification(c.config.ID)
	if err != nil {
		c.conn.Close()
		c.conn = nil
		return err
	}
	err = proto.RecvNotifyResult()
	c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) queryWinnersWithRetry() (int, error) {
	for c.running {
		if err := c.createClientSocket(); err != nil {
			return 0, err
		}
		proto := NewBetProtocol(c.conn)
		if err := proto.SendWinnersQuery(c.config.ID); err != nil {
			c.conn.Close()
			c.conn = nil
			return 0, err
		}
		status, count, err := proto.RecvWinnersResult()
		c.conn.Close()
		c.conn = nil
		if err != nil {
			return 0, err
		}
		if status == "OK" {
			return count, nil
		}
		time.Sleep(c.config.LoopPeriod)
	}
	return 0, nil
}

func (c *Client) StartClientLoop() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		log.Infof("action: signal_received | result: success | client_id: %v | signal: SIGTERM", c.config.ID)
		c.running = false
		if c.conn != nil {
			c.conn.Close()
		}
		os.Exit(0)
	}()

	rows, err := LoadBetsFromCSV(c.config.DataFilePath, c.config.ID)
	if err != nil {
		log.Criticalf("action: load_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	chunks := chunkRows(rows, c.config.BatchMaxAmount)
	for chunkIdx, chunk := range chunks {
		if !c.running {
			return
		}
		if err := c.createClientSocket(); err != nil {
			return
		}

		proto := NewBetProtocol(c.conn)
		err := proto.SendBatch(chunk)
		if err != nil {
			log.Errorf("action: send_batch | result: fail | client_id: %v | chunk: %d | error: %v",
				c.config.ID, chunkIdx, err)
			c.conn.Close()
			return
		}

		ok, count, code, err := proto.RecvBatchResult()
		c.conn.Close()
		c.conn = nil

		if err != nil {
			log.Errorf("action: receive_batch_result | result: fail | client_id: %v | chunk: %d | error: %v",
				c.config.ID, chunkIdx, err)
			return
		}
		if !ok {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | cantidad: %d | code: %s",
				c.config.ID, count, code)
			return
		}

		log.Infof("action: apuesta_enviada | result: success | client_id: %v | cantidad: %d",
			c.config.ID, count)

		time.Sleep(c.config.LoopPeriod)
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
	if err := c.notifyFinished(); err != nil {
		log.Errorf("action: notify_finished | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	count, err := c.queryWinnersWithRetry()
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", count)
}
