#!/bin/bash

#!/usr/bin/env bash
set -euo pipefail

# Resolve the directory of this script (handles symlinks)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# If we're not already running from the script's directory, switch to it
if [[ "$(pwd)" != "$SCRIPT_DIR" ]]; then
    cd "$SCRIPT_DIR"
fi

# Add ./common to PYTHONPATH (preserving any existing value)
export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"
echo "$PYTHONPATH"

# Execute the Python setup script
python scripts/setup_ilec_ddb.py
