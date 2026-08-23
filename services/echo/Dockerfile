# Multi-stage: build with the toolchain, ship without it.
FROM golang:1.27-alpine AS build

WORKDIR /src

# go.mod alone changes rarely, so this layer stays cached across source edits.
COPY go.mod ./
RUN go mod download

COPY . .

ARG VERSION=dev
# CGO off so the binary is static and runs on scratch.
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/echo .

# scratch: no shell, no package manager, nothing to exploit.
FROM scratch
COPY --from=build /out/echo /echo

# Matches runAsUser in the Deployment's securityContext.
USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/echo"]
