# Changelog

VODgrab reads this file to show release notes under **Settings → Updates**.

## 1.13.1 (2026-10-04)

- **Gentler on providers:** the catalog sync runs at most every 6 hours (the 1 to 4 hour choices are gone; daily stays the default). Installs set below 6 hours move to 6.
- **Check missing titles again** is now a dropdown: Off, or after 1, 2, 4 or 6 hours, with 2 hours as the default (was 6). It is skipped when the next scheduled sync would come first.
- Fixed the Catalog sync settings layout (the sync history was squeezed into a narrow column since 1.12.0).

## 1.13.0 (2026-10-04)

- **Pick the provider inside Dispatcharr:** with **Download through Dispatcharr** on, opening a movie asks Dispatcharr's native API which of its providers have it, and lists each copy with its quality. Leave it on **Dispatcharr picks**, or choose a provider for that download. Dispatcharr still enforces the connection limits. (Episodes still let Dispatcharr choose, since its API has no per-episode provider list.)

## 1.12.1 (2026-10-04)

- The missing-titles recheck now only re-reads the catalogs of the providers that had missing titles, instead of every provider.

## 1.12.0 (2026-10-04)

- **Quality checks when opening, split by type:** movies have their own on/off switch, and TV has **Whole season**, **First episode only** or **Don't check**.
- **Missing titles:** kept for 1 sync by default (was 3; installs still on the old default are moved to 1). New **Check missing titles again after (hours)**, default 6: when titles go missing, an extra sync runs that many hours later, and a title still gone is removed. The header shows when the recheck is due.

## 1.11.0 (2026-10-04)

- **Dispatcharr:** two checkboxes, **Download through Dispatcharr** and **Import Dispatcharr's Xtream logins as providers**. Use either or both, and turn each off again (its providers are switched off, not deleted).
- **Every profile counts:** each active Dispatcharr profile is imported as its own login with its own stream limit, so an account with two 5-stream profiles gives VODgrab 10 streams. Passwords come from Dispatcharr when it has them, and are refreshed before each sync.

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
