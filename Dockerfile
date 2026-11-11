FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
ENV GOFLAGS=-mod=vendor
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api && CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker && CGO_ENABLED=0 go build -trimpath -o /out/publisher ./cmd/publisher
RUN CGO_ENABLED=0 go test -c -o /out/storage.test ./internal/storage
FROM alpine:3.23
RUN addgroup -g 10001 app && adduser -D -u 10001 -G app app
COPY --from=build /out/ /app/
USER 10001:10001
ENTRYPOINT ["/app/api"]
