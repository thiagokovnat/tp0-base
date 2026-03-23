package common

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
)

const MaxPayload = 65535

type BetProtocol struct {
	conn net.Conn
}

func NewBetProtocol(conn net.Conn) *BetProtocol {
	return &BetProtocol{conn: conn}
}

func (p *BetProtocol) recvAll(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("recvAll: invalid length %d", n)
	}
	buf := make([]byte, n)
	read := 0
	for read < n {
		m, err := p.conn.Read(buf[read:])
		if err != nil {
			return nil, err
		}
		if m == 0 {
			return nil, fmt.Errorf("connection closed before receiving all bytes")
		}
		read += m
	}
	return buf, nil
}

func (p *BetProtocol) sendAll(data []byte) error {
	written := 0
	for written < len(data) {
		n, err := p.conn.Write(data[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("connection closed before sending all bytes")
		}
		written += n
	}
	return nil
}

func (p *BetProtocol) SendFrameText(text string) error {
	body := []byte(text)
	if len(body) == 0 || len(body) > MaxPayload {
		return fmt.Errorf("payload too large: %d bytes (max %d)", len(body), MaxPayload)
	}
	header := make([]byte, 2)
	binary.BigEndian.PutUint16(header, uint16(len(body)))
	return p.sendAll(append(header, body...))
}

func (p *BetProtocol) RecvFrameText() (string, error) {
	header, err := p.recvAll(2)
	if err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint16(header)
	if length == 0 || length > MaxPayload {
		return "", fmt.Errorf("invalid response payload length: %d", length)
	}
	payload, err := p.recvAll(int(length))
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func (p *BetProtocol) SendBatch(rows [][6]string) error {
	if len(rows) < 1 {
		return fmt.Errorf("batch must have at least one bet")
	}
	n := len(rows)
	parts := make([]string, 0, 1+6*n)
	parts = append(parts, strconv.Itoa(n))
	for _, r := range rows {
		for i := 0; i < 6; i++ {
			parts = append(parts, r[i])
		}
	}
	payload := strings.Join(parts, "\n")
	body := []byte(payload)
	if len(body) > MaxPayload {
		return fmt.Errorf("payload too large: %d bytes (max %d)", len(body), MaxPayload)
	}
	header := make([]byte, 2)
	binary.BigEndian.PutUint16(header, uint16(len(body)))
	return p.sendAll(append(header, body...))
}

func (p *BetProtocol) SendFinishedNotification(clientID string) error {
	return p.SendFrameText(fmt.Sprintf("NOTIFY_FINISHED|AGENCY=%s", clientID))
}

func (p *BetProtocol) RecvNotifyResult() error {
	text, err := p.RecvFrameText()
	if err != nil {
		return err
	}
	if !strings.HasPrefix(text, "NOTIFY_OK|") {
		return fmt.Errorf("unexpected notify response: %q", text)
	}
	return nil
}

func (p *BetProtocol) SendWinnersQuery(clientID string) error {
	return p.SendFrameText(fmt.Sprintf("QUERY_WINNERS|AGENCY=%s", clientID))
}

func (p *BetProtocol) RecvWinnersResult() (status string, count int, err error) {
	text, err := p.RecvFrameText()
	if err != nil {
		return "", 0, err
	}
	if text == "WINNERS_PENDING" {
		return "PENDING", 0, nil
	}
	parts := strings.SplitN(text, "|", 3)
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("unexpected winners response: %q", text)
	}
	if parts[0] != "WINNERS_OK" {
		return "", 0, fmt.Errorf("unexpected winners response: %q", text)
	}
	c, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("WINNERS_OK parse count: %w", err)
	}
	return "OK", c, nil
}

func (p *BetProtocol) RecvBatchResult() (ok bool, count int, code string, err error) {
	text, err := p.RecvFrameText()
	if err != nil {
		return false, 0, "", err
	}
	parts := strings.SplitN(text, "|", 3)
	if len(parts) < 2 {
		return false, 0, "", fmt.Errorf("unexpected response: %q", text)
	}
	switch parts[0] {
	case "BATCH_OK":
		c, err := strconv.Atoi(parts[1])
		if err != nil {
			return false, 0, "", fmt.Errorf("BATCH_OK parse count: %w", err)
		}
		return true, c, "", nil
	case "BATCH_FAIL":
		if len(parts) != 3 {
			return false, 0, "", fmt.Errorf("unexpected BATCH_FAIL: %q", text)
		}
		c, err := strconv.Atoi(parts[2])
		if err != nil {
			return false, 0, parts[1], fmt.Errorf("BATCH_FAIL parse count: %w", err)
		}
		return false, c, parts[1], nil
	default:
		return false, 0, "", fmt.Errorf("unexpected response: %q", text)
	}
}
