#!/bin/bash

echo "Stopping Raft cluster and Middleware gateway..."

# Find and kill the Java process running out of the middleware directory
MIDDLEWARE_PID=$(pgrep -f "middleware")
if [ -n "$MIDDLEWARE_PID" ]; then
    echo "Stopping Middleware (PID: $MIDDLEWARE_PID)..."
    kill $MIDDLEWARE_PID
else
    echo "Middleware gateway was not running."
fi

# Find and kill the Java process running out of the raft directory
RAFT_PID=$(pgrep -f "raft")
if [ -n "$RAFT_PID" ]; then
    echo "Stopping Raft cluster (PID: $RAFT_PID)..."
    kill $RAFT_PID
else
    echo "Raft cluster was not running."
fi

echo "Cleanup complete."