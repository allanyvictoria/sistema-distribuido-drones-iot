FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o drone .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/drone .
CMD ["./drone"]