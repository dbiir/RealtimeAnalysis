#!/bin/bash

if [ $# -ne 3 ]; then
    echo $0: usage: topic partitionBegin partitionEnd
    exit 1
fi

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
SBINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

topic=$1
parB=$2
parE=$3

while read -r host
do
    echo "Logging into $host, and start Loader"
    ssh $USER@$host -n "cd '$SBINDIR' && nohup ./loader-daemon.sh $topic $parB $parE > $BASEDIR/running 2>&1 &"
done < $BASEDIR/loaders
