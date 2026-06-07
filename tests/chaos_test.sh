#!/bin/bash

echo "======================================"
echo "Distributed Consensus Chaos Test"
echo "======================================"

echo ""
echo "Starting transaction stream..."
echo ""

python src/client.py 5 TX1001 Alice Bob 100 &
sleep 2

python src/client.py 5 TX1002 Bob Charlie 200 &
sleep 2

python src/client.py 5 TX1003 Charlie David 300 &
sleep 2

echo ""
echo "Stopping Node 5 (Leader)"
echo ""

docker stop node5

sleep 10

echo ""
echo "Submitting transaction after leader crash"
echo ""

python src/client.py 4 TX1004 David Emma 400

sleep 5

echo ""
echo "Restarting Node 5"
echo ""

docker start node5

sleep 10

echo ""
echo "Launching Byzantine attack"
echo ""

python src/adversary.py prepare 1

sleep 5

echo ""
echo "Launching fake COMMIT"
echo ""

python src/adversary.py commit 2

sleep 5

echo ""
echo "Submitting final transaction"
echo ""

python src/client.py 4 TX1005 Emma Frank 500

echo ""
echo "Chaos test completed"
echo ""
