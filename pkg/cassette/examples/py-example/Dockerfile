FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS build

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY main.py ./

FROM python:3.14-slim-bookworm

WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY --from=build /app/main.py ./
ENV PATH="/app/.venv/bin:$PATH" CASSETTE_LISTEN=0.0.0.0:9999
EXPOSE 9999
USER 65532:65532
ENTRYPOINT ["python", "main.py"]
