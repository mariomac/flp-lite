FROM docker.io/library/golang:1.19 as builder

WORKDIR /app

# Copy source code
COPY go.mod .
COPY go.sum .
COPY vendor/ vendor/
COPY cmd/ cmd/
COPY pkg/ pkg/

RUN go build --mod=vendor cmd/flplite/flplite.go

# final stage
FROM registry.access.redhat.com/ubi8/ubi-minimal:8.6

COPY --from=builder /app/flplite /app/

# expose ports
EXPOSE 2055

ENTRYPOINT ["/app/flplite"]
