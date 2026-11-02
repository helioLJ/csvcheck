FROM golang:1.26.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/coordinator ./cmd/coordinator \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/csvcheck ./cmd/csvcheck

FROM debian:bookworm-slim
RUN groupadd --gid 10001 csvcheck && useradd --uid 10001 --gid csvcheck --no-create-home csvcheck \
 && mkdir /data && chown csvcheck:csvcheck /data
COPY --from=build /out/ /usr/local/bin/
WORKDIR /app
COPY examples/ ./examples/
USER 10001:10001
ENV DATA_DIR=/data
ENTRYPOINT ["coordinator"]
