# syntax=docker/dockerfile:1
# NOTE: written carefully but NOT built locally (Docker is not available on
# the development machine). CGO is disabled; all binaries are pure Go.

ARG GO_VERSION=1.26.2

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY examples/webhook-receiver ./examples/webhook-receiver
ARG VERSION=0.1.0
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/ ./cmd/obsd ./cmd/loadgen ./examples/webhook-receiver

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S obs && adduser -S -G obs -h /home/obs obs \
 && mkdir -p /data && chown obs:obs /data
COPY --from=build /out/ /usr/local/bin/
USER obs
ENV OBS_LISTEN_ADDR=0.0.0.0:8428 \
    OBS_DATA_DIR=/data \
    OBS_LOG_FORMAT=json
VOLUME ["/data"]
EXPOSE 8428
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8428/healthz >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/obsd"]
