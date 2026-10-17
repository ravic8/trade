FROM golang:1.25-bookworm AS build
WORKDIR /source
RUN git init . && git remote add origin https://github.com/minio/mc.git \
    && git fetch --depth 1 origin 7394ce0dd2a80935aded936b09fa12cbb3cb8096 \
    && git checkout --detach FETCH_HEAD
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/mc .
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/mc /usr/local/bin/mc
COPY --from=build /source/LICENSE /usr/share/doc/mc/LICENSE
ENTRYPOINT ["mc"]
