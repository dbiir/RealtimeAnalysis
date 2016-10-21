#!/bin/bash

if [ $# -ne 3 ]; then
    echo $0: usage: topic partitionBegin partitionEnd
    exit 1
fi

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

topic = $1
parB = $2
parE = $3

while read -r host
do
    echo "Logging into $host, and start Loader"
    ssh $USER@$host './BASEDIR/loader-daemon.sh &'
done < $BASEDIR/../loaders
