# Multi-stage: build with the toolchain, ship without it.
FROM golang:1.27-alpine AS build

WORKDIR /src

# go.mod and go.sum change rarely, so this layer stays cached across source
# edits. relay has dependencies, unlike services/echo, so go.sum is copied too.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
# CGO off so the binary is static and runs on scratch.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/relay ./cmd/relay && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w" \
      -o /out/relay-replay ./cmd/relay-replay

# scratch: no shell, no package manager, nothing to exploit.
#
# That also means no container HEALTHCHECK -- there is no shell or curl to run
# one with. The /healthz and /readyz endpoints are for Kubernetes, which probes
# over HTTP and needs nothing inside the image.
FROM scratch

# relay dials Postgres and Kafka over TLS in the M4 (MSK) configuration, and a
# scratch image has no root certificates at all.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/relay /relay
COPY --from=build /out/relay-replay /relay-replay

# Matches runAsUser in the Deployment's securityContext.
USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/relay"]
