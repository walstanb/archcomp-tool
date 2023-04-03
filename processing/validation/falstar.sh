#!/bin/sh

HERE=`dirname "$0"`
echo HELLO
echo $HERE
echo HELLO
source $HERE/falstar-config.sh
java -cp "$MATLABJARS:$HERE/falstar.jar" falstar.Main $@
