# This image is intended to be built by GoReleaser.
FROM scratch

COPY mqtt /mqtt

USER 65532:65532

EXPOSE 1883 9464

ENTRYPOINT ["/mqtt"]
