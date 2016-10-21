#!/bin/bash

if [ $# -ne 3 ]; then
    echo $0: usage: topic partitionBegin partitionEnd
    exit 1
fi

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

topic=$1
parB=$2
parE=$3

java -jar $BASEDIR/../bin/Loader.jar $BASEDIR/../conf/config.props $topic $parB $parE
echo "Loader started... Loading from topic [$topic], partition id from [$parB] to [$parE]."
