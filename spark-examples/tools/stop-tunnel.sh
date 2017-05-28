#!/bin/bash
cd "${0%/*}/.."
kill -TERM -- -$(cat tunnel-pid.tmp)
