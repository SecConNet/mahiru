import multiprocessing

# Logging
accesslog = '-'
errorlog = '-'
capture_output = True

# Listening
bind = ['0.0.0.0:8000']

# Handling
# We need threads in a single process because we store state in memory in the
# Python process, and because we make requests to our own site at times. We use
# three times the number of cores rather than two for that reason.
worker_class = 'gthread'
workers = 1
threads = multiprocessing.cpu_count() * 3 + 1

# App
wsgi_app = 'proof_of_concept.rest.ddm_site:wsgi_app()'
