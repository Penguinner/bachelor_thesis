FROM rust:1.67
LABEL authors="felix rüdlin"

WORKDIR /usr/src/bachelor_thesis
COPY . .

RUN cargo install --path .

CMD ["bachelor_thesis", "-h"]
