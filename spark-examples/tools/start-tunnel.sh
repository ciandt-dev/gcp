#!/bin/bash

cd "${0%/*}/.."

gcloud compute ssh mykafka --ssh-flag "-NL 9092:localhost:9092" &
gcloud compute ssh mykafka --ssh-flag "-NL 2181:localhost:2181" &

echo $$ > tunnel-pid.tmp
