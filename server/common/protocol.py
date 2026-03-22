from __future__ import annotations

import socket
import struct

from common.utils import Bet

MAX_PAYLOAD = 65535


class BetProtocol:
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock

    def recv_all(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("n must be non-negative")
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed before receiving all bytes")
            buf.extend(chunk)
        return bytes(buf)

    def send_all(self, data: bytes) -> None:
        view = memoryview(data)
        while len(view):
            sent = self._sock.send(view)
            if sent == 0:
                raise ConnectionError("Connection closed before sending all bytes")
            view = view[sent:]

    def recv_bet(self) -> Bet:
        length_bytes = self.recv_all(2)
        (length,) = struct.unpack("!H", length_bytes)
        if length == 0 or length > MAX_PAYLOAD:
            raise ValueError("Invalid payload length")
        payload = self.recv_all(length)
        text = payload.decode("utf-8")
        parts = text.split("\n")
        if len(parts) != 6:
            raise ValueError("Expected exactly 6 newline-separated fields")
        agency, first_name, last_name, document, birthdate, number = parts
        try:
            return Bet(agency, first_name, last_name, document, birthdate, number)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid bet data: {e}") from e

    def send_result(self, bet: Bet) -> None:
        payload = f"SUCCESS|{bet.document}|{bet.number}".encode("utf-8")
        if len(payload) > MAX_PAYLOAD:
            raise ValueError("Response payload too large")
        header = struct.pack("!H", len(payload))
        self.send_all(header + payload)
