FROM ubuntu:26.04 AS build
RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    gnupg \
    lsb-release \
    protobuf-compiler \
    g++ \
    gcc \
    libc6-dev \
    make \
    pkg-config
RUN curl -sL https://go.dev/dl/go1.27.1.linux-amd64.tar.gz | tar -C / -xzf -
ENV PATH="/go/bin:${PATH}"
ENV GOPATH="/go"
ENV CGO_ENABLED=0

WORKDIR /src
COPY . .
RUN make build

RUN make test

FROM scratch AS base
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

FROM base AS api
COPY --from=build /src/bin/api api
CMD ["/api"]

FROM base AS idp
COPY --from=build /src/bin/idp idp
CMD ["/idp"]

FROM base AS db
COPY --from=build /src/bin/db db
CMD ["/db"]

FROM base AS artifacts
COPY --from=build /src/bin/artifacts artifacts
CMD ["/artifacts"]

FROM base AS scheduler
COPY --from=build /src/bin/scheduler scheduler
CMD ["/scheduler"]

FROM base AS pipelinesource
COPY --from=build /src/bin/pipelinesource pipelinesource
CMD ["/pipelinesource"]

FROM base AS all
COPY --from=build /src/bin/* /
