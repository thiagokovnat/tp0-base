#!/bin/bash

SERVER_CONTAINER="server"
TEST_MESSAGE="Mensaje de prueba"
EXPECTED_RESPONSE="${TEST_MESSAGE}"

if ! docker ps | grep -q "$SERVER_CONTAINER"; then
    echo "action: test_echo_server | result: fail"
    exit 1
fi

SERVER_IP="172.25.125.2"

RESPONSE=$(echo "$TEST_MESSAGE" | timeout 10 docker run --rm --network tp0_testing_net busybox:latest sh -c "echo '$TEST_MESSAGE' | nc $SERVER_IP 12345")

if [ $? -eq 0 ] && [ "$RESPONSE" = "$EXPECTED_RESPONSE" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi
