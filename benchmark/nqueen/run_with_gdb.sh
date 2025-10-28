#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 [num_nodes] [board_size]"
  echo "Example: $0 8 14"
  exit 1
fi

NUM_NODES=$1
BOARD_SIZE=${2:-15}

# Create a gdb command file in the current directory
cat > gdb_commands.txt <<'EOF'
set pagination off
set logging file gdb_backtrace.txt
set logging on
run
bt
bt full
info registers
info threads
thread apply all bt
quit
EOF

echo "Running with GDB to capture crash information..."
srun --exclusive --nodes=${NUM_NODES} --ntasks-per-node=1 --cpus-per-task=64 --propagate=CORE \
  bash -c "cd /qfs/people/baek071/ARTS-private/benchmark/nqueen && gdb -batch -x gdb_commands.txt --args ./nqueen_arts_explicit ${BOARD_SIZE}"

echo ""
echo "Backtrace saved to gdb_backtrace.txt"
