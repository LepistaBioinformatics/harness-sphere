# HarnessSphere image for the zombie-crab stack.
#
# Two stages: build the binary, then ship it on a minimal runtime with nothing
# else. The service holds NO Docker socket and needs no /proc or /sys bind —
# sysinfo reads /proc/meminfo and /proc/stat, which Docker does not namespace, so
# the host layer works from inside the container as-is.

# The toolchain file says `stable`, which is not a pin. Naming a version here is
# deliberate: a release image should not change because a build happened on a
# different day. 1.96 is the version the workspace has been exercised against.
FROM rust:1.96-slim AS builder

WORKDIR /src

# Build deps for the OTLP exporter's transitive C dependencies.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# `otlp` is the only feature. The `ingest` and `prometheus` features no longer
# exist: nothing in this stack pushes OTLP at us, and nothing exposes an
# exposition endpoint — picoclaw's HTTP surface is /health, /ready and /reload.
RUN cargo build --release --bin harnesssphere --features otlp

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin harnesssphere

COPY --from=builder /src/target/release/harnesssphere /usr/local/bin/harnesssphere
COPY --from=builder /src/config.zombie-crab.toml /etc/harnesssphere/config.toml

# Non-root, and it must stay that way. This is a passive observer: if a task ever
# needs root here, the task is wrong.
USER 10001:10001

ENTRYPOINT ["/usr/local/bin/harnesssphere"]
CMD ["/etc/harnesssphere/config.toml"]
