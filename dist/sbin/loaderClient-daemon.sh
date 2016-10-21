#!/bin/bash

if [ $# -ne 5 ]; then
    echo $0: usage: baseFile topic speed length sf
    exit 1
fi

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

baseFile=$1
topic=$2
speed=$3
length=$4
sf=$5

java -jar $BASEDIR/../bin/LoaderClient.jar $BASEDIR/../conf/config.props $baseFile $topic $speed $length $sf
echo "LoaderClient started... Reading from [$baseFile], loading into kafka topic [$topic] expected speed is [$speed], length of each record is around [$length], scale factor is [$sf]."
