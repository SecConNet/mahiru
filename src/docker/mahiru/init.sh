#!/bin/bash

pid_file='/var/run/gunicorn/gunicorn.pid'

function stop_container {
    echo 'Caught TERM signal, shutting down' 1>&2
    gunicorn_pid=$(cat ${pid_file})
    kill -TERM $(cat ${pid_file})
}

# Docker sends us a SIGTERM on 'docker stop'.
# This catches it and shuts down cleanly by calling the function above
trap stop_container SIGTERM

echo 'Installed handler' 1>&2

gunicorn -c /etc/gunicorn.conf.py --pid ${pid_file} &

echo 'Started gunicorn' 1>&2

wait

rm -f ${pid_file}

echo 'Exiting' 1>&2
