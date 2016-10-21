#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
SBINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

while read -r host
do
    echo "Logging into $host, and start Loader"
    ssh $USER@$host -n "cd '$SBINDIR' && nohup ./loader-daemon.sh > $BASEDIR/running 2>&1 &"
done < $BASEDIR/loaders
