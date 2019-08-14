#!/bin/bash

export HOST_IP=`hostname -i`

if [ "$BIND_ON_LOCALHOST" == true ] || [ "$BIND_ON_IP" == "127.0.0.1" ]; then
    export BIND_ON_IP="127.0.0.1"
    export HOST_IP="127.0.0.1"
elif [ -z "$BIND_ON_IP" ]; then
    # not binding to localhost and bind_on_ip is empty - use default host ip addr
    export BIND_ON_IP=$HOST_IP
elif [ "$BIND_ON_IP" != "0.0.0.0" ]; then
    # binding to a user specified addr, make sure HOST_IP also uses the same addr
    export HOST_IP=$BIND_ON_IP
fi

# this env variable is deprecated
export BIND_ON_LOCALHOST=false

exec "$@"
