FROM alpine:3.9

USER nobody

ADD build/_output/atomix-nopaxos-sequencer /usr/local/bin/atomix-nopaxos-sequencer

ENTRYPOINT ["atomix-nopaxos-sequencer"]
