# STAGE 1: Rust builder
FROM rust:latest AS rust_builder

WORKDIR /usr/src/bachelor_thesis
COPY Cargo.toml .
COPY src ./src
RUN cargo install --path . --all-features

# Stage 2: Runtime enviornment
FROM ubuntu:latest
LABEL authors="Felix Rüdlin"

WORKDIR /usr/src/bachelor_thesis

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-venv \
    libssl3 \
    curl \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://get.docker.com | sh

# Install qlever
RUN python3 -m venv /usr/qlever-venv && \
	/usr/qlever-venv/bin/pip install --no-cache-dir qlever

# Copy needed files
COPY --from=rust_builder /usr/local/cargo/bin/bachelor_thesis /usr/local/bin/bachelor_thesis
COPY --from=rust_builder /usr/src/bachelor_thesis/src/data /data

ENV PATH="/usr/qlever-venv/bin:$PATH"
ENV RUST_BACKTRACE=1

#CMD ["bachelor_thesis", "-h"]
CMD ["bachelor_thesis", "src/data/dblp.tsv", "dblp", "-p", "-r", "-a"]
