import socket
import logging
import signal
import sys

from common.protocol import BetProtocol
from common.utils import store_bets


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True 
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
        Receives a bet from the client and stores it

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            proto = BetProtocol(client_sock)
            bet = proto.recv_bet()
            store_bets([bet])
            logging.info(
                f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}'
            )
            proto.send_result(bet)
            logging.debug(
                f'action: bet_response_sent | result: success | ip: {addr[0]} | dni: {bet.document}'
            )
        except (ValueError, ConnectionError, OSError) as e:
            logging.error(f'action: receive_bet | result: fail | error: {e}')
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
