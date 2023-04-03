#!/bin/sh

HERE=`dirname "$0"`
echo HELLLO
echo $HERE
echo HELLO
source $HERE/falstar-config.sh
java -cp "$MATLABJARS:$HERE/falstar.jar" falstar.util.Matlab $@
