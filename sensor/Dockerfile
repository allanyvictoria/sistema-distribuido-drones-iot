FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o sensor .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/sensor .
CMD ["./sensor"]