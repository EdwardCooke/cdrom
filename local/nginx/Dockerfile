FROM nginx:latest
RUN mkdir -p /certs && \
    openssl genpkey -algorithm RSA -out /certs/cert.key && \
    openssl req -new -x509 -key /certs/cert.key -out /certs/cert.crt -days 365 -subj "/CN=localhost"
COPY nginx.conf /etc/nginx/nginx.conf