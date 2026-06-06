import os
import json
import time
import socket
import threading
from collections import defaultdict

from crypto_utils import (
    CryptoUtils,
    PBFTMessage
)

HEARTBEAT_INTERVAL = 2
ELECTION_TIMEOUT = 6

MODE_PAXOS = "PAXOS"
MODE_PBFT = "PBFT"


class Node:

    def __init__(
        self,
        node_id,
        host,
        port,
        peers,
        mode=MODE_PAXOS
    ):

        self.node_id = str(node_id)

        self.host = host
        self.port = port

        self.peers = peers

        self.mode = mode

        self.running = True

        self.is_leader = False

        self.current_leader = None

        self.last_heartbeat = time.time()

        self.ledger = []

        self.promises = defaultdict(set)
        self.accepted = defaultdict(set)

        self.pbft_prepare = defaultdict(set)
        self.pbft_commit = defaultdict(set)

        self.proposal_number = 0

        self.sequence_number = 0

        self.server_socket = None

        self.private_path = f"keys/{self.node_id}_private.pem"
        self.public_path = f"keys/{self.node_id}_public.pem"

        if not os.path.exists(self.private_path):
            CryptoUtils.generate_key_pair(self.node_id)

        self.private_key = CryptoUtils.load_private_key(
            self.private_path
        )

        self.public_key = CryptoUtils.load_public_key(
            self.public_path
        )

        self.peer_public_keys = {}

        self.load_peer_keys()

        self.log_file = f"ledger_{self.node_id}.log"

    def load_peer_keys(self):

        for peer in self.peers:

            peer_id = str(peer["id"])

            path = f"keys/{peer_id}_public.pem"

            if os.path.exists(path):

                self.peer_public_keys[peer_id] = (
                    CryptoUtils.load_public_key(path)
                )

    def start(self):

        print(
            f"[{self.node_id}] Starting node..."
        )

        threading.Thread(
            target=self.server_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=self.heartbeat_loop,
            daemon=True
        ).start()

        threading.Thread(
            target=self.failure_detector,
            daemon=True
        ).start()

        while self.running:
            time.sleep(1)

    def server_loop(self):

        self.server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.server_socket.bind(
            (self.host, self.port)
        )

        self.server_socket.listen()

        print(
            f"[{self.node_id}] Listening on {self.port}"
        )

        while self.running:

            conn, addr = self.server_socket.accept()

            threading.Thread(
                target=self.handle_connection,
                args=(conn,),
                daemon=True
            ).start()

    def handle_connection(self, conn):

        try:

            data = conn.recv(65535)

            if not data:
                return

            message = json.loads(
                data.decode()
            )

            self.process_message(
                message
            )

        except Exception as e:

            print(
                f"[{self.node_id}] Error: {e}"
            )

        finally:

            conn.close()

    def send_message(
        self,
        peer_host,
        peer_port,
        message
    ):

        try:

            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            sock.connect(
                (peer_host, peer_port)
            )

            sock.send(
                json.dumps(message).encode()
            )

            sock.close()

        except Exception as e:

            print(
                f"[{self.node_id}] Send failed: {e}"
            )

    def broadcast(self, message):

        for peer in self.peers:

            self.send_message(
                peer["host"],
                peer["port"],
                message
            )

    def process_message(self, message):

        msg_type = message.get("type")

        if msg_type == "HEARTBEAT":
            self.handle_heartbeat(message)

        elif msg_type == "ELECTION":
            self.handle_election(message)

        elif msg_type == "LEADER":
            self.handle_new_leader(message)

        elif msg_type == "PREPARE":
            self.handle_prepare(message)

        elif msg_type == "PROMISE":
            self.handle_promise(message)

        elif msg_type == "ACCEPT":
            self.handle_accept(message)

        elif msg_type == "ACCEPTED":
            self.handle_accepted(message)

        elif msg_type == "PRE_PREPARE":
            self.handle_pre_prepare(message)

        elif msg_type == "PBFT_PREPARE":
            self.handle_pbft_prepare(message)

        elif msg_type == "COMMIT":
            self.handle_commit(message)

        elif msg_type == "CLIENT_TX":
            self.handle_client_tx(message)

    def heartbeat_loop(self):

        while self.running:

            if self.is_leader:

                heartbeat = {
                "type": "HEARTBEAT",
                "leader": self.node_id,
                "timestamp": time.time()
            }

            self.broadcast(
                heartbeat
            )

        time.sleep(
            HEARTBEAT_INTERVAL
        )
def handle_heartbeat(self, message):

    leader = message.get(
        "leader"
    )

    self.current_leader = leader

    self.last_heartbeat = time.time()

    print(
        f"[{self.node_id}] Heartbeat from leader {leader}"
    )

def failure_detector(self):

    while self.running:

        if self.is_leader:

            time.sleep(1)
            continue

        elapsed = (
            time.time()
            - self.last_heartbeat
        )

        if elapsed > ELECTION_TIMEOUT:

            print(
                f"[{self.node_id}] Leader timeout detected"
            )

            self.start_election()

        time.sleep(1)

    def start_election(self):

        print(
            f"[{self.node_id}] Starting election"
        )

        higher_found = False

        election_msg = {
            "type": "ELECTION",
            "candidate": self.node_id
        }

        my_id = int(
            self.node_id
        )

        for peer in self.peers:

            peer_id = int(
                peer["id"]
            )

            if peer_id > my_id:

                higher_found = True

                self.send_message(
                    peer["host"],
                    peer["port"],
                    election_msg
                )

        if not higher_found:

            self.become_leader()

    def handle_election(self, message):

        candidate = int(
            message["candidate"]
        )

        my_id = int(
            self.node_id
        )

        if my_id > candidate:

            reply = {
                "type": "ELECTION_OK",
                "node": self.node_id
            }

            sender = None

            for peer in self.peers:

                if int(peer["id"]) == candidate:

                    sender = peer
                    break

            if sender:

                self.send_message(
                    sender["host"],
                    sender["port"],
                    reply
                )

            threading.Thread(
                target=self.start_election,
                daemon=True
            ).start()