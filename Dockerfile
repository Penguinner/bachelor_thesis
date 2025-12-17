# STAGE 1: Rust builder
FROM rust:latest AS rust_builder

WORKDIR /usr/src/bachelor_thesis
COPY Cargo.toml .
COPY src ./src
RUN cargo install --path . --all-features

# Stage 2: osm2rdf
FROM ubuntu:latest AS osm2rdf_builder
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y git g++ libexpat1-dev cmake libbz2-dev libomp-dev zlib1g-dev
RUN mkdir /app && git clone https://github.com/ad-freiburg/osm2rdf.git /app
RUN cd /app/ && mkdir -p build && cd build && cmake .. && make -j


# Stage 3: Runtime enviornment
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
    ca-certificates \
    osm2pgsql \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://get.docker.com | sh

# Copy needed files
COPY --from=rust_builder /usr/local/cargo/bin/bachelor_thesis /usr/local/bin/bachelor_thesis
COPY --from=rust_builder /usr/src/bachelor_thesis/src/data /usr/src/bachelor_thesis
COPY --from=osm2rdf_builder /app/build/apps/osm2rdf /usr/osm2rdf_builder

ENV PATH="/usr/qlever-venv/bin:$PATH"
ENV PATH="/usr/osm2rdf:$PATH"
ENV RUST_BACKTRACE=1

#CMD ["bachelor_thesis", "-h"]
CMD ["bachelor_thesis", "dblp.tsv", "dblp", "-q", "-p", "-d", "-r", "-a"]
