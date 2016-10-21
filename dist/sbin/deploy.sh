#!/bin/bash

PREFIXDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && cd ".." && pwd )"
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

while read -r host
do
    if [ "$host" = "$HOSTNAME" ]; then
        echo "Skip local host ."
    else
        echo "deploying to [$host] ..."
        scp -r $BASEDIR $host:$PREFIXDIR/
    fi
done < $BASEDIR/loaders
