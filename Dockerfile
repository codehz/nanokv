FROM alpine:latest AS builder

ENV XMAKE_ROOT=y
RUN apk add --no-cache curl p7zip unzip git gcc g++ make cmake xmake perl linux-headers
ADD ./xmake.lua /root/nanokv/xmake.lua
WORKDIR /root/nanokv
RUN xmake f -y --mode=release
ADD . /root/nanokv
RUN xmake -v
RUN xmake install

FROM alpine:latest
RUN apk add --no-cache libatomic libstdc++ libgcc
COPY --from=builder /usr/local/bin/nanokv /usr/local/bin/nanokv
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/nanokv", "-d", "/data/db"]