#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

java -jar $BASEDIR/bin/Loader.jar $BASEDIR/conf/config.props &
echo "Loader started...."
