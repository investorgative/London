#!/usr/bin/env bash
for s in London LondonWeb LondonCloud; do
  tmux kill-session -t "$s" 2>/dev/null && echo "stopped $s" || echo "$s not running"
done
