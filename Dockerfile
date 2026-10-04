FROM python:3.12-slim

# ffprobe verifies finished downloads; VODgrab falls back to size checks without it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tini \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    VODGRAB_DATA=/data \
    VODGRAB_DOCKER=1 \
    PUID=1000 \
    PGID=1000

WORKDIR /app
COPY --chmod=755 vodgrab.py docker/healthcheck.py /app/
COPY --chmod=755 docker/entrypoint.sh /entrypoint.sh

VOLUME ["/data"]
EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python3", "/app/healthcheck.py"]

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["serve"]
