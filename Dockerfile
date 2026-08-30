FROM --platform=$BUILDPLATFORM golang:1.25.4-alpine3.21 AS builder
MAINTAINER xtaci <daniel820313@gmail.com>
LABEL org.opencontainers.image.source=https://github.com/smithwhere/kcptun
ENV GO111MODULE=on
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
RUN apk add git
WORKDIR /workspace
COPY . .
RUN go mod download
RUN export CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} && \
	go build -mod=readonly -trimpath -ldflags "-X main.VERSION=$(date -u +%Y%m%d) -s -w" -o /client ./client && \
	go build -mod=readonly -trimpath -ldflags "-X main.VERSION=$(date -u +%Y%m%d) -s -w" -o /server ./server

FROM alpine:3.18
RUN apk add --no-cache iptables
COPY --from=builder /client /bin
COPY --from=builder /server /bin
EXPOSE 29900/udp
EXPOSE 12948
