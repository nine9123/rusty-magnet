# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --system --uid 1000 --home /data rustymagnet && mkdir -p /data && chown rustymagnet /data
COPY --from=build /src/target/release/rustymagnet /usr/local/bin/rustymagnet
USER rustymagnet
WORKDIR /data
ENV RUSTYMAGNET_DB=/data/rustymagnet.db \
    RUSTYMAGNET_HTTP=0.0.0.0:3333 \
    RUSTYMAGNET_DHT_PORT=3334
EXPOSE 3333/tcp 3334/udp
VOLUME ["/data"]
ENTRYPOINT ["rustymagnet"]
CMD ["run"]
