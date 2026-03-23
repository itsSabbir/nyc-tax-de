# =============================================================================
# Dockerfile: Custom Airflow image with dbt pre-installed
# =============================================================================
# We can't use the vanilla Airflow image because our DAG runs dbt commands.
# This Dockerfile starts from the official Airflow image and adds:
#   1. git (needed by dbt to download packages like dbt_utils)
#   2. dbt-postgres (the dbt adapter for Postgres databases)
#
# The image is built automatically by Docker Compose (see compose.yml).
# You only need to rebuild if you change this file:
#   docker compose -f docker/compose.yml build
# =============================================================================

# Start from the official Apache Airflow image (Python 3.12, Debian-based)
FROM apache/airflow:2.9.3

# Switch to root user temporarily to install system packages
# (the "airflow" user doesn't have sudo/apt permissions)
USER root

# Install git — dbt needs it to download packages from GitHub
# --no-install-recommends = skip optional extras to keep the image small
# apt-get clean + rm = clean up package cache to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends git \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the "airflow" user for security
# (never run Airflow as root in production — it's a security risk)
USER airflow

# Install dbt with the Postgres adapter
# --no-cache-dir = don't cache pip downloads (keeps image smaller)
# We pin the version (1.8.2) so builds are reproducible. If you need a
# newer version, update this number and rebuild the image.
RUN pip install --no-cache-dir dbt-postgres==1.8.2
