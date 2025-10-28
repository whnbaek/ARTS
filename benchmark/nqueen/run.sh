#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 [num_nodes] [board_size]"
  echo "Example: $0 8 14"
  exit 1
fi

NUM_NODES=$1
BOARD_SIZE=${2:-1}

srun --exclusive --nodes=${NUM_NODES} --ntasks-per-node=1 --cpus-per-task=64 ./nqueen_arts_explicit ${BOARD_SIZE}