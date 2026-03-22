import logging
import re
import signal
import socket
import sys

from common.protocol import BetProtocol
from common.utils import store_bets

_INVALID_BATCH_SIZE_RE = re.compile(r"invalid batch size:\s*(\d+)", re.IGNORECASE)


def _batch_count_from_value_error(err: ValueError) -> int:
    msg = str(err)
    m = _INVALID_BATCH_SIZE_RE.search(msg)
    return int(m.group(1)) if m else 0


class Server:
    def __init__(self, port, listen_backlog, batch_max_amount: int):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._batch_max_amount = batch_max_amount
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

    def __handle_client_connection(self, client_sock):
        """
        Receives a batch of bets from the client and stores them atomically.

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        proto = BetProtocol(client_sock)
        try:
            addr = client_sock.getpeername()
            bets = proto.recv_batch(self._batch_max_amount)
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
                except (ConnectionError, OSError) as send_err:
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
                except (ConnectionError, OSError) as send_err:
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
