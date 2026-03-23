import logging
import re
import signal
import socket
import sys
from typing import Optional

from common.protocol import BetProtocol
from common.utils import has_won, load_bets, store_bets

_INVALID_BATCH_SIZE_RE = re.compile(r"invalid batch size:\s*(\d+)", re.IGNORECASE)
_AGENCY_RE = re.compile(r"(?:agency|agencia|client|cliente|id)\D*(\d+)", re.IGNORECASE)
_INT_RE = re.compile(r"\b(\d+)\b")


def _batch_count_from_value_error(err: ValueError) -> int:
    msg = str(err)
    m = _INVALID_BATCH_SIZE_RE.search(msg)
    return int(m.group(1)) if m else 0


class Server:
    def __init__(self, port, listen_backlog, batch_max_amount: int, n_agencies: int):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._batch_max_amount = batch_max_amount
        self._n_agencies = n_agencies
        self._notified_agencies: set[int] = set()
        self._sorted_done = False
        self._winners_by_agency: dict[int, list[str]] = {}
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info('action: signal_received | result: success | signal: SIGTERM')
        self._running = False
        self._server_socket.close()

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._running:
                    logging.error(f'action: accept_connection | result: fail | error: {e}')
                break
        sys.exit(0)

    def __extract_agency(self, text: str) -> Optional[int]:
        match = _AGENCY_RE.search(text)
        if match:
            agency = int(match.group(1))
            if 1 <= agency <= self._n_agencies:
                return agency
        for match in _INT_RE.finditer(text):
            agency = int(match.group(1))
            if 1 <= agency <= self._n_agencies:
                return agency
        return None

    def __is_winners_query(self, text: str) -> bool:
        lowered = text.lower()
        return (
            "ganador" in lowered
            or "winner" in lowered
            or "consulta" in lowered
            or "winners" in lowered
        )

    def __is_finish_notification(self, text: str) -> bool:
        lowered = text.lower()
        return (
            "finish" in lowered
            or "final" in lowered
            or "notify" in lowered
            or "notific" in lowered
            or "done" in lowered
            or "complete" in lowered
        )

    def __run_draw_if_ready(self):
        if self._sorted_done:
            return
        if len(self._notified_agencies) < self._n_agencies:
            return
        winners_by_agency: dict[int, list[str]] = {}
        for bet in load_bets():
            if has_won(bet):
                winners_by_agency.setdefault(bet.agency, []).append(bet.document)
        self._winners_by_agency = winners_by_agency
        self._sorted_done = True
        logging.info('action: sorteo | result: success')

    def __handle_client_connection(self, client_sock):
        proto = BetProtocol(client_sock)
        try:
            addr = client_sock.getpeername()
            text = proto.recv_frame_text()
            bets = proto.try_parse_batch_payload(text, self._batch_max_amount)
            if bets is None:
                agency = self.__extract_agency(text)
                if agency is None:
                    proto.send_frame_text("ERROR|INVALID_AGENCY")
                    return
                if self.__is_winners_query(text):
                    if not self._sorted_done:
                        proto.send_frame_text("WINNERS_PENDING")
                        return
                    winners = self._winners_by_agency.get(agency, [])
                    count = len(winners)
                    winners_payload = ",".join(winners)
                    proto.send_frame_text(f"WINNERS_OK|{count}|{winners_payload}")
                    return
                if self.__is_finish_notification(text):
                    logging.info(f"action: finish_notification | result: success | agency: {agency}")
                    self._notified_agencies.add(agency)
                    self.__run_draw_if_ready()
                    proto.send_frame_text(f"NOTIFY_OK|{agency}")
                    return
                proto.send_frame_text("ERROR|UNKNOWN_ACTION")
                return
            n = len(bets)
            try:
                store_bets(bets)
            except OSError as store_err:
                logging.error(f'action: store_bets | result: fail | error: {store_err}')
                logging.info(
                    f'action: apuesta_recibida | result: fail | cantidad: {n}'
                )
                try:
                    proto.send_batch_result(False, n, "STORE")
                except (ConnectionError, OSError, ValueError) as send_err:
                    logging.error(f'action: send_batch_result | result: fail | error: {send_err}')
                return
            for bet in bets:
                logging.info(
                    f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}'
                )
            logging.info(f'action: apuesta_recibida | result: success | cantidad: {n}')
            proto.send_batch_result(True, n)
            logging.debug(
                f'action: batch_response_sent | result: success | ip: {addr[0]} | cantidad: {n}'
            )
        except ValueError as e:
            msg = str(e)
            if msg.lower().startswith("invalid batch size"):
                count = _batch_count_from_value_error(e)
                logging.error(msg)
                logging.info(
                    f'action: apuesta_recibida | result: fail | cantidad: {count}'
                )
                try:
                    proto.send_batch_result(False, count, "INVALID")
                except (ConnectionError, OSError, ValueError) as send_err:
                    logging.error(f'action: send_batch_result | result: fail | error: {send_err}')
            else:
                logging.error(f'action: receive_batch | result: fail | error: {e}')
        except (ConnectionError, OSError) as e:
            logging.error(f'action: receive_batch | result: fail | error: {e}')
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
