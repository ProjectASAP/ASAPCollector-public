FROM ubuntu:24.04

COPY asap-precompute-rs/target/release/asap-otap-demo /usr/local/bin/asap-otap-demo
COPY benchmarks/run-pipeline-component.sh /usr/local/bin/run-pipeline-component

ENV ASAP_DEMO_BIN=/usr/local/bin/asap-otap-demo
ENTRYPOINT ["/usr/local/bin/run-pipeline-component"]
