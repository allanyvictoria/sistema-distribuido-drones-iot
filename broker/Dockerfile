FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o broker .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/broker .
CMD ["./broker"]