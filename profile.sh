#!/bin/bash

# Replace with the path to FlameGraph on you own machine.
# FlameGraph can be downloaded from here: https://github.com/brendangregg/FlameGraph
FLAMEDIR=~/Work/FlameGraph

# Some utility variables
EXEC=$(basename $1)
CMDLINE=$@
STACKCOLLAPSE=$FLAMEDIR/stackcollapse.pl
FLAMEGRAPH=$FLAMEDIR/flamegraph.pl

##       ============ Native code MacOS =============
##
## Profile your executable `a.out` as follows
##
##   ./profile.sh a.out n A.txt B.txt
##
## The above command will produce a file profile.svg that you can view in your browser
## Uncomment the following lines
sudo rm profile.svg /tmp/out.stacks
sudo dtrace \
    -c "$CMDLINE" \
    -o /tmp/out.stacks \
    -n "profile-997 /execname == \"$EXEC\"/ { @[ustack(997)] = count(); }" > /dev/null
$STACKCOLLAPSE /tmp/out.stacks | $FLAMEGRAPH > profile.svg

