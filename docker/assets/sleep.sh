#!/bin/bash

# Catch SIGTERM sent by "docker stop" and shut down cleanly
function stop_container {
    stopping=true
}

trap stop_container SIGTERM

while [ -z ${stopping} ] ; do
    sleep 1
done

