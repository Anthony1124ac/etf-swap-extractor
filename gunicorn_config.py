# Gunicorn configuration file
timeout = 300  # 5 minutes
workers = 1
worker_class = 'sync'
keepalive = 120
max_requests = 1000
max_requests_jitter = 50 