# Use Python 3.13 slim image as base
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

# # Setup a non-root user
# RUN groupadd --system --gid 999 nonroot \
#  && useradd --system --gid 999 --uid 999 --create-home nonroot

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Install curl for heathcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev
RUN uv pip install "pandera[pandas]"

# Create config directory if it doesn't exist
RUN mkdir -p /app/config

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

# Use the non-root user to run our application
# USER nonroot

# Health check
HEALTHCHECK CMD curl -f http://localhost:8000/health

# Run app
CMD ["uv", "run", "main.py"]