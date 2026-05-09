FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o sensor-manual .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/sensor-manual .
CMD ["./sensor-manual"]