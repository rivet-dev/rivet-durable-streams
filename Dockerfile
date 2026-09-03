FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --locked --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/target/release/rivet-durable-streams ./rivet-durable-streams
COPY inspector ./inspector
ENV PORT=3000
EXPOSE 3000
CMD ["./rivet-durable-streams", "--host", "0.0.0.0"]
