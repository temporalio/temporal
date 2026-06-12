ARG ALPINE_TAG

FROM alpine:${ALPINE_TAG}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && \
    apk upgrade --no-cache zlib && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy all admin tool binaries:
# - temporal (CLI)
# - temporal-cassandra-tool
# - temporal-sql-tool
# - temporal-elasticsearch-tool
# - tdbg
COPY --chmod=755 \
    ./build/${TARGETARCH}/temporal \
    ./build/${TARGETARCH}/temporal-cassandra-tool \
    ./build/${TARGETARCH}/temporal-sql-tool \
    ./build/${TARGETARCH}/temporal-elasticsearch-tool \
    ./build/${TARGETARCH}/tdbg \
    /usr/local/bin/

COPY ./build/temporal/schema /etc/temporal/schema

USER temporal

# Keep the container running idle so users can exec into it for admin tasks.
#
#   trap exit INT HUP TERM
#     Register a signal handler so that when the shell receives SIGINT, SIGHUP,
#     or SIGTERM it runs "exit" instead of the default PID 1 behavior (ignore).
#
#   sleep infinity &
#     Start a never-ending process to keep the container alive. It runs in the
#     background ("&") so the shell remains the foreground process.
#
#   wait
#     Block the shell until background jobs finish. Unlike a foreground "sleep",
#     "wait" is a shell builtin that gets interrupted when a signal arrives,
#     giving the shell a chance to run the trap handler and exit immediately.
#
# Without the "& wait" pattern, the shell is blocked on the foreground sleep and
# never processes signals, causing the container to hang until the kubelet
# termination deadline before being force-killed.
CMD ["sh", "-c", "trap exit INT HUP TERM; sleep infinity & wait"]
