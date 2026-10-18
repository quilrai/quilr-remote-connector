# Build stage
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked --bin quilr-connector

# Runtime stage
FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /app/target/release/quilr-connector /usr/local/bin/quilr-connector

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/quilr-connector"]
