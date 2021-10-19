# Mahiru pilot container image.

# To execute a workflow step, Mahiru use a do-nothing pilot container which
# starts up first and provides a network namespace to set up the network in,
# before we start the actual compute asset. This image is the image for that
# pilot.

# Eventually, this should be a from scratch image with a small C program that
# just sits and waits for a shutdown signal, but for now we'll do the
# equivalent using a bash script. We have ubuntu:latest already anyway for
# the other containers.
FROM ubuntu:latest

COPY sleep.sh /usr/local/bin/sleep.sh
RUN chmod +x /usr/local/bin/sleep.sh

CMD ["/usr/local/bin/sleep.sh"]
