# Distributed Consensus Engine

### IIT Jodhpur – Fundamentals of Distributed Systems

### Assignment 1

## Overview

This project implements a resilient distributed state machine replication system using:

* Leader Election (Bully Algorithm)
* Paxos Consensus (Crash Fault Tolerance)
* Practical Byzantine Fault Tolerance (PBFT)
* Cryptographic Message Signing
* Byzantine Adversary Simulation
* Docker-based Deployment
* Chaos Testing

The system maintains an append-only distributed transaction ledger across a cluster of 5 nodes.

---

## Features

### Mode A – Crash Fault Tolerance

* Leader Election using Bully Algorithm
* Heartbeat-based Failure Detection
* Paxos Consensus

  * Prepare
  * Promise
  * Accept
  * Accepted
* Ledger persistence

Supports up to 2 node failures.

---

### Mode B – Byzantine Fault Tolerance

* PBFT Consensus

  * Pre-Prepare
  * Prepare
  * Commit
* RSA Digital Signatures
* Signature Verification
* Byzantine Node Detection

Supports 1 malicious node.

---

## Project Structure

distributed-consensus-engine/

├── src/

│   ├── node.py

│   ├── client.py

│   ├── adversary.py

│   └── crypto_utils.py

│

├── tests/

│   └── chaos_test.sh

│

├── Dockerfile

├── docker-compose.yml

├── requirements.txt

├── README.md

└── project_report.pdf

---

## Installation

Create virtual environment:

```bash
python -m venv venv
```

Activate:

```bash
venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Running Nodes Locally

Terminal 1

```bash
python src/node.py 1
```

Terminal 2

```bash
python src/node.py 2
```

Terminal 3

```bash
python src/node.py 3
```

Terminal 4

```bash
python src/node.py 4
```

Terminal 5

```bash
python src/node.py 5
```

Expected:

```text
[5] ELECTED LEADER
```

---

## Submit Transaction

Example:

```bash
python src/client.py 5 TX1001 Alice Bob 500
```

Expected:

```text
PREPARE
PROMISE
ACCEPT
ACCEPTED
COMMITTED
```

---

## Run Byzantine Attack

Fake Prepare:

```bash
python src/adversary.py prepare 1
```

Fake Commit:

```bash
python src/adversary.py commit 2
```

Expected:

```text
Unknown sender 999
```

or

```text
Invalid PREPARE signature
```

---

## Docker Deployment

Build:

```bash
docker-compose build
```

Start:

```bash
docker-compose up
```

Stop:

```bash
docker-compose down
```

---

## Chaos Testing

Run:

```bash
chmod +x tests/chaos_test.sh
./tests/chaos_test.sh
```

The script performs:

* Transaction submission
* Leader crash
* Leader re-election
* Byzantine attack
* Recovery validation

---

## Paxos Flow

Client

↓

Leader

↓

PREPARE

↓

PROMISE

↓

ACCEPT

↓

ACCEPTED

↓

COMMIT

↓

Ledger Update

---

## PBFT Flow

Client

↓

Primary

↓

PRE-PREPARE

↓

PREPARE

↓

COMMIT

↓

Ledger Update

---

## Security

Cryptographic protection is implemented using:

* RSA Key Pairs
* Digital Signatures
* Signature Verification

This prevents message spoofing and unauthorized transaction injection.

---

## Author

Ved Prakash (G25AI1052)

Indian Institute of Technology Jodhpur

PG Diploma – Data Engineering
