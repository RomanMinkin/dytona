#!/bin/bash

go test -p 1 $1 $(go list ./... | grep -v "vendor") | \
sed ''/PASS/s//$(printf "\033[32mPASS\033[0m")/'' | \
sed ''/FAIL/s//$(printf "\033[31mFAIL\033[0m")/'' | \
sed ''/ok/s//$(printf "\033[32mOK\033[0m")/''
