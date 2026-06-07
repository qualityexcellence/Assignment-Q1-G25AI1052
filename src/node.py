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

        self.highest_prepare = 0

        self.accepted_proposal = None

        self.accepted_value = None

        self.pending_transactions = {}

        self.committed = set()

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

        # PBFT state

        self.pbft_sequence = 0

        self.pbft_preprepare = {}

        self.pbft_prepare = defaultdict(set)

        self.pbft_commit = defaultdict(set)

        self.pbft_executed = set()

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

    def handle_prepare(self, message):

        proposal_id = message["proposal_id"]

        proposer = message["proposer"]

        tx = message["transaction"]

        if proposal_id < self.highest_prepare:

            return

        self.highest_prepare = proposal_id

        self.pending_transactions[
            proposal_id
        ] = tx

        promise = {
            "type": "PROMISE",
            "proposal_id": proposal_id,
            "from": self.node_id,
            "accepted_proposal":
                self.accepted_proposal,
            "accepted_value":
                self.accepted_value
        }

        for peer in self.peers:

            if str(peer["id"]) == proposer:

                self.send_message(
                    peer["host"],
                    peer["port"],
                    promise
                )
                break

        print(
            f"[{self.node_id}] PROMISE -> {proposal_id}"
        )

    def handle_promise(self, message):

        proposal_id = message["proposal_id"]

        sender = message["from"]

        self.promises[
            proposal_id
        ].add(sender)

        majority = (
            len(self.peers) + 1
        ) // 2 + 1

        count = len(
            self.promises[
                proposal_id
            ]
        )

        print(
            f"[{self.node_id}] Promise count={count}"
        )

        if count < majority:
            return

        tx = self.pending_transactions.get(
            proposal_id
        )

        if not tx:
            return

        accept = {
            "type": "ACCEPT",
            "proposal_id": proposal_id,
            "transaction": tx,
            "proposer": self.node_id
        }

        self.accepted[proposal_id] = {
            self.node_id
        }

        self.broadcast(
            accept
        )

        print(
            f"[{self.node_id}] ACCEPT broadcast"
        )

    def handle_accept(self, message):

        proposal_id = message["proposal_id"]

        tx = message["transaction"]

        proposer = message["proposer"]

        if proposal_id < self.highest_prepare:

            return

        self.accepted_proposal = proposal_id

        self.accepted_value = tx

        accepted = {
            "type": "ACCEPTED",
            "proposal_id": proposal_id,
            "from": self.node_id
        }

        for peer in self.peers:

            if str(peer["id"]) == proposer:

                self.send_message(
                    peer["host"],
                    peer["port"],
                    accepted
                )
                break

        print(
            f"[{self.node_id}] ACCEPTED"
        )

    def handle_accepted(self, message):

        proposal_id = message["proposal_id"]

        sender = message["from"]

        self.accepted[
            proposal_id
        ].add(sender)

        majority = (
            len(self.peers) + 1
        ) // 2 + 1

        count = len(
            self.accepted[
                proposal_id
            ]
        )

        print(
            f"[{self.node_id}] Accepted count={count}"
        )

        if count < majority:
            return

        tx = self.pending_transactions.get(
            proposal_id
        )

        if not tx:
            return

        commit = {
            "type": "PAXOS_COMMIT",
            "proposal_id": proposal_id,
            "transaction": tx
        }

        self.broadcast(
            commit
        )

        self.commit_transaction(
            proposal_id,
            tx
        )

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

        elif msg_type == "ELECTION_OK":
            self.handle_election_ok(message)

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

        elif msg_type == "PAXOS_COMMIT":
            self.handle_paxos_commit(message)

    def handle_commit(self, message):

        sender = str(
            message["sender"]
        )

        if sender not in self.peer_public_keys:
            return

        public_key = (
            self.peer_public_keys[
                sender
            ]
        )

        if not PBFTMessage.verify(
            public_key,
            message
        ):
            return

        seq = message["sequence"]

        self.pbft_commit[
            seq
        ].add(sender)

        commit_count = len(
            self.pbft_commit[
                seq
            ]
        )

        if commit_count < 3:
            return

        if seq in self.pbft_executed:
            return

        self.pbft_executed.add(
            seq
        )

        self.commit_pbft_transaction(
            seq,
            message["payload"]
        )

    def commit_pbft_transaction(
        self,
        sequence,
        transaction
    ):

        self.ledger.append(
            transaction
        )

        with open(
            self.log_file,
            "a"
        ) as f:

            f.write(
                json.dumps(
                    {
                        "sequence":
                            sequence,
                        "tx":
                            transaction
                    }
                ) + "\n"
            )

        print(
            f"[{self.node_id}] PBFT COMMITTED "
            f"SEQ={sequence}"
        )

    def detect_equivocation(
        self,
        sequence,
        digest
    ):

        if sequence not in self.pbft_preprepare:
            return False

        old_digest = (
            self.pbft_preprepare[
                sequence
            ]["digest"]
        )

        return old_digest != digest

    def handle_pre_prepare(self, message):

        sender = str(
            message["sender"]
        )

        if sender not in self.peer_public_keys:
            return

        public_key = (
            self.peer_public_keys[
                sender
            ]
        )

        valid = PBFTMessage.verify(
            public_key,
            message
        )

        if not valid:

            print(
                f"[{self.node_id}] INVALID SIGNATURE"
            )
            return

        seq = message["sequence"]

        self.pbft_preprepare[
            seq
        ] = message

        prepare = PBFTMessage.create_message(
            "PBFT_PREPARE",
            self.node_id,
            seq,
            message["payload"]
        )

        signed_prepare = (
            PBFTMessage.sign(
                self.private_key,
                prepare
            )
        )

        self.broadcast(
            signed_prepare
        )

        print(
            f"[{self.node_id}] PREPARE {seq}"
        )

    def handle_paxos_commit(self, message):

        proposal_id = message[
            "proposal_id"
        ]

        tx = message[
            "transaction"
        ]

        self.commit_transaction(
            proposal_id,
            tx
        )

    def commit_transaction(
        self,
        proposal_id,
        transaction
    ):

        if proposal_id in self.committed:
            return

        self.committed.add(
            proposal_id
        )

        self.ledger.append(
            transaction
        )

        with open(
            self.log_file,
            "a"
        ) as f:

            f.write(
                json.dumps(
                    transaction
                ) + "\n"
            )

        print(
            f"[{self.node_id}] COMMITTED {transaction}"
        )

    def start_paxos(self, transaction):
        print(f"[{self.node_id}] paxos start {transaction}")
            
    def handle_client_tx(self, message):

        tx = message["transaction"]

        if not self.is_leader:
            return

        if self.mode == MODE_PAXOS:

            self.start_paxos(tx)

        elif self.mode == MODE_PBFT:

            self.start_pbft(tx)

    def start_pbft(self, transaction):

        self.pbft_sequence += 1

        seq = self.pbft_sequence

        msg = PBFTMessage.create_message(
            "PRE_PREPARE",
            self.node_id,
            seq,
            transaction
        )

        signed_msg = PBFTMessage.sign(
            self.private_key,
            msg
        )

        self.pbft_preprepare[seq] = signed_msg

        self.broadcast(
            signed_msg
        )

        print(
            f"[{self.node_id}] PRE-PREPARE {seq}"
        )

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

    def handle_election_ok(self, message):

        print(
            f"[{self.node_id}] Higher node exists"
        )

        self.last_heartbeat = time.time()

    def become_leader(self):

        self.is_leader = True

        self.current_leader = self.node_id

        print(
            f"[{self.node_id}] ELECTED LEADER"
        )

        leader_msg = {
            "type": "LEADER",
            "leader": self.node_id
        }

        self.broadcast(
            leader_msg
        )

    def handle_new_leader(self, message):

        leader = message["leader"]

        self.current_leader = leader

        self.is_leader = (
            leader == self.node_id
        )

        self.last_heartbeat = time.time()

        print(
            f"[{self.node_id}] New leader = {leader}"
        )

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

if __name__ == "__main__":
    print("Integrated node.py scaffold created.")