# A simple Mahiru compute asset.

# This creates a simple compute asset which calculates the minimum and maximum
# of a list of numbers. Inputs and output are JSON files.
FROM mahiru-test/compute-asset-base

USER root
RUN \
    apt-get update && \
    apt-get -y install python3-requests && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY app.py /home/mahiru/app.py
RUN \
    chown mahiru:mahiru /home/mahiru/app.py && \
    chmod +x /home/mahiru/app.py
USER mahiru
