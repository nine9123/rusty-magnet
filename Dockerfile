# Both base images are overridable so a build host that can't reach Docker Hub
# can use a mirror or a copy in its own registry, e.g.
#   --build-arg BASE_IMAGE=gitea.internal:30008/nine9/debian:bookworm-slim
ARG RUST_IMAGE=rust:1-slim-bookworm
ARG BASE_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM ${BASE_IMAGE}
RUN useradd --system --uid 1000 --home /data rustymagnet && mkdir -p /data && chown rustymagnet /data
COPY --from=build /src/target/release/rustymagnet /usr/local/bin/rustymagnet
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh
# Starts as root only to make the database directory writable (a bind mount
# Docker created is owned by root), then drops to PUID:PGID; see the script.
WORKDIR /data
ENV RUSTYMAGNET_DB=/data/rustymagnet.db \
    RUSTYMAGNET_HTTP=0.0.0.0:3333 \
    RUSTYMAGNET_DHT_PORT=3334 \
    PUID=1000 \
    PGID=1000
EXPOSE 3333/tcp 3334/udp
VOLUME ["/data"]
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
