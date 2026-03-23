from __future__ import annotations

import socket
import struct
from typing import Optional

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

    def recv_frame(self) -> bytes:
        length_bytes = self.recv_all(2)
        (length,) = struct.unpack("!H", length_bytes)
        if length == 0 or length > MAX_PAYLOAD:
            raise ValueError("invalid batch size: 0 (bad payload length)")
        return self.recv_all(length)

    def recv_frame_text(self) -> str:
        return self.recv_frame().decode("utf-8")

    def send_frame_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        if len(payload) > MAX_PAYLOAD:
            raise ValueError("Response payload too large")
        header = struct.pack("!H", len(payload))
        self.send_all(header + payload)

    def parse_batch_payload(self, text: str, max_batch_size: int) -> list[Bet]:
        text = text.rstrip("\n\r")
        lines = text.split("\n")
        if not lines:
            raise ValueError("invalid batch size: 0 (empty payload)")
        try:
            n = int(lines[0].strip())
        except ValueError as e:
            raise ValueError("invalid batch size: 0 (could not parse count line)") from e

        if n < 1:
            raise ValueError(f"invalid batch size: {n} (must be at least 1)")
        if n > max_batch_size:
            raise ValueError(
                f"invalid batch size: {n} (exceeds maximum {max_batch_size})"
            )
        expected_lines = 1 + 6 * n
        if len(lines) != expected_lines:
            raise ValueError(
                f"invalid batch size: {n} (expected {expected_lines} lines, got {len(lines)})"
            )
        bets: list[Bet] = []
        for i in range(n):
            base = 1 + i * 6
            agency, first_name, last_name, document, birthdate, number = lines[base : base + 6]
            try:
                bets.append(Bet(agency, first_name, last_name, document, birthdate, number))
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"invalid batch size: {n} (invalid bet fields: {e})"
                ) from e
        return bets

    def try_parse_batch_payload(self, text: str, max_batch_size: int) -> Optional[list[Bet]]:
        lines = text.rstrip("\n\r").split("\n")
        if not lines:
            return None
        try:
            int(lines[0].strip())
        except ValueError:
            return None
        return self.parse_batch_payload(text, max_batch_size)

    def recv_batch(self, max_batch_size: int) -> list[Bet]:
        payload = self.recv_frame()
        text = payload.decode("utf-8")
        return self.parse_batch_payload(text, max_batch_size)

    def send_batch_result(self, success: bool, count: int, error_code: Optional[str] = None) -> None:
        if success:
            payload = f"BATCH_OK|{count}".encode("utf-8")
        else:
            code = error_code or "ERROR"
            payload = f"BATCH_FAIL|{code}|{count}".encode("utf-8")
        if len(payload) > MAX_PAYLOAD:
            raise ValueError("Response payload too large")
        header = struct.pack("!H", len(payload))
        self.send_all(header + payload)
