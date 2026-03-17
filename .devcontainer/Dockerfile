# https://hub.docker.com/_/python
# Cf. https://luis-sena.medium.com/creating-the-perfect-python-dockerfile-51bdec41f1c8
FROM python:3.12-slim AS base

# Setup env
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
# Keep Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
# Enable Python tracebacks on segfaults
ENV PYTHONFAULTHANDLER=1
# Turn off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

RUN \
  # Install dependencies
  apt-get update \
  && apt-get install -y --no-install-recommends unzip \
  # update pip
  && pip install --upgrade pip \
  # remove unneeded libs
  && apt-get autoremove -y \
  # cleanup
  && rm -rf /var/lib/apt/lists/* \
;

# run as non-root user, for schmecurity!
#RUN useradd --create-home kaxauser
#USER kaxauser
# Path of our app inside the container
RUN mkdir /app
WORKDIR /app


FROM base AS dev_env
# Development environment setup

COPY pyproject.toml ./
COPY README.md ./
COPY src/ ./src/
COPY tests/ ./tests/

# install dependencies
RUN \
  pip install pdm \
  && pdm install --global -G:all --frozen-lockfile --project . \
;


FROM dev_env AS final_env
# Final runnable stage


CMD ["python"]
