# A simple data asset containing a trivial JSON file
FROM mahiru-test/data-asset-base:latest

RUN echo '[1, 21, 2, 8, 5, 3, 13, 1, 34]' >/var/www/data.json
