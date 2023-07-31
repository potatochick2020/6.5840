#!/bin/bash

# check if the arguments are valid
if [ $# -ne 2 ]; then
  echo "Usage: $0 [Times] [2A/2B/2C/2D - Case Insensitive]"
  exit 1
fi

# assign the arguments to variables 
times=$1
test=${2^^}

# loop for the given number of times
for ((i=1; i<=times; i++))
do
  # print the current iteration
  echo "----------------------------------------" >> result.txt
  echo "Iteration $i" >> result.txt
  # execute the command and append the output to result.txt
  go test -run $test -race >> result.txt
done