#!/bin/bash

# Run middleware setup in an isolated subshell background process
echo "Starting Middleware gateway..."
(cd middleware && source run.sh) &

# Run raft setup in an isolated subshell background process
echo "Starting Raft cluster..."
(cd raft && source run.sh) &

# Keep the root script alive and wait for both background processes
echo "Both services are initializing."
wait