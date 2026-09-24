FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY asap-stream-bench /usr/local/bin/asap-stream-bench
ENTRYPOINT ["/usr/local/bin/asap-stream-bench"]
