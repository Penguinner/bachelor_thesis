# STAGE 1: Rust builder
FROM rust:latest AS rust_builder

WORKDIR /usr/src/bachelor_thesis
COPY Cargo.toml .
COPY src ./src
RUN cargo install --path .

# Stage 2: Qlever builder
FROM python:3.11-slim AS qlever_builder

RUN python3 -m venv /opt/qlever-venv && \
	/opt/qlever-venv/bin/pip install --no-cache-dir qlever

# Stage 3: Runtime enviornment
FROM ubuntu:latest
LABEL authors="Felix Rüdlin"

WORKDIR /usr/src/bachelor_thesis

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    libssl3 \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy needed files
COPY --from=rust_builder /usr/local/cargo/bin/bachelor_thesis /usr/local/bin/bachelor_thesis
COPY --from=qlever_builder /opt/qlever-venv /opt/qlever-venv
COPY --from=rust_builder /usr/src/bachelor_thesis/src/data ./src/data

ENV PATH="/opt/qlever-venv/bin:$PATH"
ENV RUST_BACKTRACE=1

#CMD ["bachelor_thesis", "-h"]
CMD ["bachelor_thesis", "src/data/dblp.tsv", "dblp", "-d", "-r", "-a"]
