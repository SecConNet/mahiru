#!/bin/bash

# Catch SIGTERM sent by "docker stop" and shut down cleanly
function stop_container {
    service nginx stop
    stopping=true
}

trap stop_container SIGTERM

# Start WebDAV server and wait until we're done
service nginx start

while [ -z ${stopping} ] ; do
    sleep 1
done

