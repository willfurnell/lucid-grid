#!/usr/bin/env bash

# Get status of all jobs in current directory

for i in $( grep -rl "https" output_* ); do
    glite-ce-job-status --endpoint ice.esc.qmul.ac.uk:8443 --input $i
done