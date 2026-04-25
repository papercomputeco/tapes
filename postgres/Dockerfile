# syntax=docker/dockerfile:1

ARG PG_MAJOR=17
ARG CNPG_IMAGE=ghcr.io/cloudnative-pg/postgresql:17.7-standard-bookworm
ARG PGDUCKDB_IMAGE=pgduckdb/pgduckdb:17-v1.1.1

FROM ${PGDUCKDB_IMAGE} AS pgduckdb
ARG PG_MAJOR

FROM ${CNPG_IMAGE}
ARG PG_MAJOR
ARG TARGETARCH

USER root

ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/postgresql/${PG_MAJOR}/bin" \
    PGDATA="/var/lib/postgresql/data" \
    LANG="en_US.utf8"

# Copy pg_duckdb runtime artifacts from the official pg_duckdb image onto the
# CloudNativePG standard image
COPY --from=pgduckdb /usr/lib/postgresql/${PG_MAJOR}/lib/libduckdb.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=pgduckdb /usr/lib/postgresql/${PG_MAJOR}/lib/pg_duckdb.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=pgduckdb /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/pg_duckdb /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/pg_duckdb
COPY --from=pgduckdb /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/pg_duckdb.index.bc /usr/lib/postgresql/${PG_MAJOR}/lib/bitcode/
COPY --from=pgduckdb /usr/share/postgresql/${PG_MAJOR}/extension/pg_duckdb* /usr/share/postgresql/${PG_MAJOR}/extension/

# Copy the standard postgres docker entrypoint bits so this image can be used
# both by CloudNativePG and by simple local `docker run` workflows.
COPY --from=pgduckdb /usr/local/bin/docker-entrypoint.sh /usr/local/bin/
COPY --from=pgduckdb /usr/local/bin/docker-ensure-initdb.sh /usr/local/bin/
COPY --from=pgduckdb /usr/local/bin/docker-enforce-initdb.sh /usr/local/bin/
COPY --from=pgduckdb /usr/local/bin/gosu /usr/local/bin/

# Stage libnss_wrapper from whichever multiarch dir the pgduckdb base image
# ships, then place it under the destination image's triplet (TARGETARCH is
# amd64/arm64, not the Debian multiarch triplet, so it gets mapped below).
COPY --from=pgduckdb /usr/lib/*-linux-gnu/libnss_wrapper.so /tmp/libnss_wrapper.so

RUN case "${TARGETARCH}" in \
      amd64) triplet=x86_64-linux-gnu ;; \
      arm64) triplet=aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    install -m 0644 /tmp/libnss_wrapper.so /usr/lib/${triplet}/libnss_wrapper.so && \
    rm /tmp/libnss_wrapper.so && \
    apt-get update && \
    apt-get install -y --no-install-recommends libcurl4 && \
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false && \
    rm -rf /var/lib/apt/lists/* /var/cache/* /var/log/* && \
    echo "shared_preload_libraries='pg_duckdb'" >> /usr/share/postgresql/${PG_MAJOR}/postgresql.conf.sample && \
    mkdir -p /docker-entrypoint-initdb.d "${PGDATA}" /var/run/postgresql && \
    chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/docker-ensure-initdb.sh /usr/local/bin/docker-enforce-initdb.sh /usr/local/bin/gosu && \
    chown -R 26:26 /docker-entrypoint-initdb.d "${PGDATA}" /var/run/postgresql && \
    chmod 03775 /var/run/postgresql

COPY postgres/initdb.d/0000-install-extensions.sql /docker-entrypoint-initdb.d/

USER 26

EXPOSE 5432
VOLUME ["/var/lib/postgresql/data"]

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["postgres", "-c", "listen_addresses=*"]
