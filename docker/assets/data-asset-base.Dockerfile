# Base image for Mahiru data assets.

# In Mahiru, data assets are containers containing a server serving data.
# This builds a base image containing a WebDAV server serving simple files,
# as a kind of minimal viable data asset. Files can be read and written, and
# no files are included, so that this can be used as a base for making concrete
# data assets with data, or as an empty container to save step outputs to.
FROM ubuntu:latest

RUN \
    apt-get update && \
    apt-get -y dist-upgrade && \
    apt-get install -y nginx-full && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY nginx.conf /etc/nginx/sites-available/default
RUN chown -R www-data:www-data /var/www
COPY init.sh /usr/local/bin/init.sh
RUN chmod +x /usr/local/bin/init.sh

CMD ["/usr/local/bin/init.sh"]
