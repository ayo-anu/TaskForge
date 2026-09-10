# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded AS uv


FROM python:3.12.14-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254 AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/taskforge/.venv

COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN uv lock --check
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-default-groups --no-editable
RUN rm /opt/taskforge/.venv/.lock \
    && chmod -R go-w /opt/taskforge/.venv


FROM python:3.12.14-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254 AS runtime

ENV PATH=/opt/taskforge/.venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/taskforge/.venv

RUN groupadd --gid 10001 taskforge \
    && useradd --uid 10001 --gid taskforge --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin taskforge

WORKDIR /opt/taskforge
COPY --from=builder /opt/taskforge/.venv /opt/taskforge/.venv

USER 10001:10001
STOPSIGNAL SIGTERM


FROM runtime AS api

CMD ["python", "-m", "taskforge.api"]


FROM runtime AS worker

CMD ["python", "-m", "taskforge.worker"]
