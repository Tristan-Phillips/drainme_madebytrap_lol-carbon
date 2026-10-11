# Build: digest-pinned (stack-pitfalls P6). The Go version matches go.mod.
FROM golang:1.26-alpine@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static by assertion: modernc.org/sqlite is pure Go, so the binary runs on scratch (D51). The page in web/ is
# compiled in with go:embed (D50).
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /drainme ./cmd/drainme

# Runtime: the binary and CA roots for the RPC endpoints, nothing else. Everything runs in UTC, so no tzdata.
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /drainme /drainme
USER 65532:65532
ENTRYPOINT ["/drainme"]
CMD ["serve"]
