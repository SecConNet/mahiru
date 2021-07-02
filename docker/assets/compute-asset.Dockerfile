# A simple Mahiru compute asset.

# This creates a simple compute asset which calculates the minimum and maximum
# of a list of numbers. Inputs and output are JSON files.
FROM mahiru-test/compute-asset-base

USER root
COPY app.py /home/mahiru/app.py
RUN \
    chown mahiru:mahiru /home/mahiru/app.py && \
    chmod +x /home/mahiru/app.py

USER mahiru
RUN pip3 install --user requests
