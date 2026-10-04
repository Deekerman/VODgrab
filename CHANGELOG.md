# Changelog

VODgrab reads this file to show release notes under **Settings → Updates**.

## 1.10.0 (2026-10-04)

- **Dispatcharr:** connect Dispatcharr in Settings with its address and an API key. Then either use Dispatcharr as a provider (downloads go through it, so they count toward each account's connection limit together with live TV), or import its Xtream accounts as providers (you enter each password, since Dispatcharr does not share them).
- **Fix wrong Radarr links:** a movie's details now show which Radarr movie it is linked to, with the IMDb and TMDB IDs. Press **Not this movie** to unlink it, or **Link to a different movie** to pick the right one from your Radarr library. VODgrab then offers it to Radarr, and lists it on Wanted, only for the right movie.

## 1.9.1 (2026-10-04)

- App icon in the browser tab, and for shortcuts added to a phone's home screen.

## 1.9.0 (2026-10-04)

- **Download all logs:** VODgrab now keeps its log in `data/logs/` (up to about 25 MB) and Settings → Log has a **Download all logs** button.
- **Sync change lists:** every catalog sync saves the titles it added and removed. Download them from Settings with **Download last sync changes**, or with the **Changes** link next to each sync. The last 30 syncs are kept.
- **In-app updates:** Settings → Updates shows when a new version is out, with these notes. Installs that run from the Python file can update with one click. Docker installs show the `docker compose pull` command.
- New logo in the app header.

## 1.8.2 (2026-10-04)

- Fixed Radarr or Sonarr grabbing the same release over and over. VODgrab's download history now filters by Radarr/Sonarr before applying the limit, so a busy Sonarr no longer hides Radarr's downloads before they are imported.
- If Radarr or Sonarr grabs a release whose earlier download is still on disk, VODgrab reuses that file instead of downloading it again.
- If a download has not been imported after 30 minutes, VODgrab logs the reason Radarr or Sonarr gives, and shows it in History.
- Catalog syncs report new and removed titles, per provider and in total.
- A warning when a provider suddenly returns no movies or series.

## 1.8.1

- First public release, with the Docker image.
