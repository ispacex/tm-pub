# The tm monitor: Python stdlib and an ssh client, nothing else.
# The configuration is mounted at /app/tm.toml (see deploy/docker-compose.yml).
FROM python:3.12-alpine
RUN apk add --no-cache openssh-client-default \
 && adduser -D -H -u 10001 tm
WORKDIR /app
COPY tm /app/tm
ENV PYTHONUNBUFFERED=1 TM_CONFIG=/app/tm.toml TM_DATA=/data TM_LISTEN=0.0.0.0:8080
USER tm
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
CMD ["python", "-m", "tm.server"]
