# VODgrab

Download Xtream IPTV VOD as real video files for Sonarr and Radarr.

VODgrab is a single Python file (standard library only) that serves:

- a **web UI** for providers, the catalog, the queue and settings
- a **Newznab indexer** that Sonarr and Radarr search
- a **SABnzbd-compatible API** that Sonarr and Radarr send downloads to

`ffprobe` (from ffmpeg) is optional. VODgrab uses it to verify finished downloads, and the Docker image includes it.

## Docker Compose

```yaml
services:
  vodgrab:
    image: ghcr.io/deekerman/vodgrab:latest
    container_name: vodgrab
    restart: unless-stopped
    ports:
      - "8765:8765"
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=America/Toronto
    volumes:
      - ./data:/data
      - /path/to/downloads:/downloads
```

```sh
docker compose up -d
```

Then open `http://<host>:8765`.

To build the image yourself instead of pulling it, run `docker compose up -d --build` (the compose file in this repo has `build: .`).

### Volumes

| Container path | Purpose |
| --- | --- |
| `/data` | Database, settings, metadata cache and backups. Keep this. |
| `/downloads` | Downloads go to `/downloads/iptv` by default (the **Base path** setting). |

**Sonarr and Radarr must see the downloads at the same path.** Mount the same host folder at `/downloads` in all three containers. Then no remote path mapping is needed, and imports work.

### Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `PUID` / `PGID` | `1000` | User and group VODgrab runs as. Match Sonarr and Radarr. Use `0` to run as root. |
| `TZ` | UTC | Time zone for schedules and logs. |
| `VODGRAB_DATA` | `/data` | Data directory inside the container. |

On start, the container sets `/data` to be owned by `PUID:PGID`. If `/downloads/iptv` doesn't exist yet, it creates that folder too. Nothing else under `/downloads` is changed.

### Connecting Sonarr and Radarr

When everything runs on the same compose network:

1. In VODgrab **Settings**, set the Sonarr URL to `http://sonarr:8989` and the Radarr URL to `http://radarr:7878`, and add their API keys.
2. VODgrab can add itself to Sonarr and Radarr as an indexer and download client. To add them by hand, use host `vodgrab`, port `8765`, and the API key shown in VODgrab.

## Without Docker

```sh
python3 vodgrab.py serve          # run the web UI, indexer and download API
sudo python3 vodgrab.py install   # install and start a systemd service
python3 vodgrab.py sync           # run one catalog sync and exit
```

Data is stored in `./data` next to the script, or in `$VODGRAB_DATA` if that's set.
