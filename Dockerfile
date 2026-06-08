# ---- build stage ----
FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /bin/wackgrok-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /bin/wackgrok-client ./cmd/client

# ---- runtime stage ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /bin/wackgrok-server /usr/local/bin/wackgrok-server
COPY --from=builder /bin/wackgrok-client /usr/local/bin/wackgrok-client

# control | data | http
EXPOSE 7070 7071 80

ENTRYPOINT ["wackgrok-server"]
