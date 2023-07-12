#!/bin/bash

# check if the arguments are valid
if [ $# -ne 1 ]; then
  echo "Usage: $0 command times"
  exit 1
fi

# assign the arguments to variables 
times=$1

# loop for the given number of times
for ((i=1; i<=times; i++))
do
  # execute the command and append the output to result.txt
  go test -run 2A -race >> result.txt
done