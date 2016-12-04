#!/bin/sh

status=0

FILES=$(find . -type f -name "*.go" | grep -v "vendor")

for file in $FILES; do
   badfile="$(gofmt -l $file)"
   if test -n "$badfile" ; then
       echo "gpfmt check failed: file needs gofmt: $file"
       status=1
   fi
done
exit $status