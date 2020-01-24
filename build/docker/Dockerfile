FROM alpine:3.9

USER nobody

ADD build/_output/nopaxos-proxy /usr/local/bin/nopaxos-proxy

ENTRYPOINT ["nopaxos-proxy"]
