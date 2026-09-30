FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends python3 util-linux && rm -rf /var/lib/apt/lists/*

COPY asap-otap-demo /usr/local/bin/asap-otap-demo
COPY run-pipeline-component.sh /usr/local/bin/run-pipeline-component
COPY summarize-stages.py /usr/local/benchmarks/summarize-stages.py

ENV ASAP_DEMO_BIN=/usr/local/bin/asap-otap-demo
ENTRYPOINT ["/usr/local/bin/run-pipeline-component"]
