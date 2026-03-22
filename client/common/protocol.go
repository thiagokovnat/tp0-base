package common

import (
	"encoding/binary"
	"fmt"
	"net"
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

func (p *BetProtocol) SendBet(agencyID, nombre, apellido, documento, nacimiento, numero string) error {
	payload := strings.Join([]string{
		agencyID,
		nombre,
		apellido,
		documento,
		nacimiento,
		numero,
	}, "\n")
	body := []byte(payload)
	if len(body) == 0 || len(body) > MaxPayload {
		return fmt.Errorf("invalid payload length: %d", len(body))
	}
	header := make([]byte, 2)
	binary.BigEndian.PutUint16(header, uint16(len(body)))
	return p.sendAll(append(header, body...))
}

func (p *BetProtocol) RecvResult() (ok bool, dni string, numero string, err error) {
	header, err := p.recvAll(2)
	if err != nil {
		return false, "", "", err
	}
	length := binary.BigEndian.Uint16(header)
	if length == 0 || length > MaxPayload {
		return false, "", "", fmt.Errorf("invalid response payload length: %d", length)
	}
	payload, err := p.recvAll(int(length))
	if err != nil {
		return false, "", "", err
	}
	text := string(payload)
	parts := strings.SplitN(text, "|", 3)
	if len(parts) != 3 || parts[0] != "SUCCESS" {
		return false, "", "", fmt.Errorf("unexpected response: %q", text)
	}
	return true, parts[1], parts[2], nil
}
