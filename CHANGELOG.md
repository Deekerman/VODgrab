# Changelog

VODgrab reads this file to show release notes under **Settings → Updates**.

## 1.16.5 (2026-11-09)

- **Fixed: downloads stopping for good.** If the database was busy at the moment a download started (for example during a catalog rebuild), its download slot was reserved but the download never ran, so the queue could stall until a restart. Resume and Retry couldn't help. Now nothing is reserved until the download really starts, and any slot without a running download is freed automatically.
- **Shorter database pauses.** A catalog sync or recheck where every provider failed (like a 503) no longer rebuilds the title list. A real rebuild now keeps the database locked for much less time, and status updates and Sonarr/Radarr webhooks wait out a short lock instead of failing.
- **"Import failed: Manually imported 1 files"** is explained now. VODgrab checks whether the episode or movie really got the file: if it did (Sonarr copied it), the download counts as imported. If not, the reason says Sonarr or Radarr skipped it, usually because its quality profile doesn't allow that quality.
- **No more daily re-grabs of missing titles.** When a provider answers "title not found (404)" to a download, that copy isn't offered to Sonarr or Radarr for a week (it was a day), so they stop grabbing the same dead copy every day.
- **History: Retry failed** button. Retries every failed download and failed import at once, each release once, and skips releases that were downloaded or queued again since.

## 1.16.4 (2026-11-08)

- **Fixed: "Sonarr can't see …" / "Radarr can't see …" on files they can see.** To read a download's quality, VODgrab asked Sonarr or Radarr about the folder and passed the show or movie ID. With an ID they list that title's *existing* files instead of the folder, so the new file never showed up. VODgrab now asks about the file itself, then imports it into the exact episode or movie. Press **Retry import** in History on the ones that failed.
- Until now that same mix-up meant manual downloads quietly fell back to Sonarr and Radarr guessing from the file name. The exact import from 1.16.0 (and the length checks) now really run.
- Wanted and History build their Open in Radarr/Sonarr links in one go instead of one lookup per row, and replies are compressed with the fastest setting, which is easier on low-power servers.

## 1.16.3 (2026-11-08)

- **Release group.** Radarr and Sonarr showed VODgrab downloads with no release group, because names ended in `.IPTV`. Set one in **Settings → Folders → Release group** (for example `VODgrab`), or give each provider its own in its card. Names then end in `-VODgrab`, and Radarr and Sonarr show it as the release group, including on manual imports (using the provider the file actually came from). Leave it blank to keep the old names.
- **Every failed import is in the log now, with the reason.** Before, a refusal from Radarr or Sonarr only showed in History, and an unexpected error could leave a download stuck on Importing.
- **Clear message when Radarr or Sonarr can't see the download folder**, for example: "Radarr can't see /downloads/iptv/manual/movies/… Give Radarr the same folder at the same path." Once fixed, press **Retry import** in History; there's no need to download again.
- **Movies get the length check too:** a "movie" far shorter than Radarr's runtime (a 1-minute trailer or placeholder) is not imported, and the reason says to try another provider's copy.
- **Wanted:** "Import failed" and "Download failed" are short now; click them to drop down the full reason and a link to History.

## 1.16.2 (2026-11-07)

- **Fix a show linked to the wrong Sonarr series.** A show's details now have the **Linked in Sonarr** section that movies already had: **Not this show** unlinks it, and **Link to a different show** picks the right one from your Sonarr library. Unlinking also cancels manual downloads that were still waiting or downloading for the wrong show.
- **Wrong-show episodes are not imported.** Since 1.16.0 VODgrab tells Sonarr exactly which episode a file is for, so a wrong link would put the wrong show's file in. Now an episode whose length is far off from the show's usual episode length in Sonarr (less than half, or more than double) is refused with the reason. This catches cases like a 55-minute drama and a 23-minute cartoon with the same name.
- **Better automatic matching:** two titles with the same name and year are no longer treated as the same title when both have a TMDB ID and the IDs differ.
- If **Retry import** in 1.16.0 or 1.16.1 put another show's episodes into a Sonarr show (for example Bordertown), delete those files in Sonarr.

## 1.16.1 (2026-11-07)

- **Open in Radarr / Open in Sonarr** buttons: in a movie's or show's details (also next to each title under **Linked in Radarr**), on Wanted rows, and in History, so you can jump straight to the item Radarr or Sonarr is looking at, for example to see why an import was refused.
- New optional **Address for links** for Radarr and Sonarr in Settings. Fill it in when the URL VODgrab uses doesn't open in your browser (for example `http://radarr:7878` inside Docker): use the address you normally type, such as `http://192.168.1.10:7878` or your reverse-proxy URL.
- Sonarr show links appear after VODgrab's next check of your Sonarr library (at startup, then hourly).

## 1.16.0 (2026-11-07)

- **Manual downloads import into the right episode.** VODgrab used to hand Sonarr the file and let it work out the episode from the file name. For shows with scene numbering on TheXEM, or several shows with the same title, Sonarr picked the wrong episode (often one that already had a file, giving "Not an upgrade") or none at all ("Failed to import"). VODgrab now tells Sonarr or Radarr exactly which episode or movie the file is for. If that episode or movie already has a file, only a higher-resolution file replaces it, and the reason is shown otherwise.
- **Fix earlier failed imports:** in **History**, press **Retry import** on them. The retry uses the new exact import.
- **Wanted stays up to date.** A movie or episode leaves the Wanted page as soon as it is imported, instead of at the next hourly check. Items you downloaded manually show their status: queued, downloading, or **Import failed** with the reason (click it to open History).
- **Much faster with a big Radarr or Sonarr library.** Every Browse card and Wanted row checked "is this in Radarr/Sonarr" and "did VODgrab download it" by reading the whole library and download history. With 12,000 movies in Radarr, the Wanted page took 5 seconds (now 0.03 s), a Browse page up to 0.7 s (now about 0.01 s), and the Browse "Missing" filter almost 6 minutes (now 0.08 s). The Wanted check runs about 2.5 times faster.
- **Smaller and fewer downloads for the browser:** pages, lists and Sonarr/Radarr indexer replies are compressed (the app page goes from 140 KB to 50 KB), the browser keeps the app page until VODgrab is updated, and a tab in the background stops checking for updates until you look at it again.
- **Faster for Sonarr and Radarr:** new database indexes for the indexer's RSS feed and the download-client checks they make every minute. The Wanted check fetches episode lists from your providers a few at a time instead of one by one, and only asks the next provider when the better one doesn't have the episode. A Radarr search loads the details of every provider's copy at once.
- Fewer error lines in the log: a page closed before it finished loading no longer logs a traceback, and the background metadata fill waits instead of failing while a catalog sync is writing.

## 1.15.0 (2026-11-07)

- **Wanted: download manually.** Next to **Search in Radarr** and **Search N in Sonarr** there is now **Download manually** (per movie, per show, per episode, and for everything listed). It skips the Radarr or Sonarr search, so quality profiles and indexer settings can't get in the way: VODgrab downloads the best provider's copy right away (falling back to the others), then Radarr or Sonarr imports the file for the exact movie or series on the list. Rows show when they were queued, and pressing twice doesn't download twice.

## 1.14.0 (2026-10-16)

- **Unreachable providers no longer cause endless retries.** After two connection failures in a row (name not found, refused, timed out), a provider is marked down. VODgrab stops using it for downloads, quality checks and lookups, stops offering its copies to Radarr and Sonarr (so they don't grab them over and over), and checks it again after 15 min, 30 min, 1 h, 2 h, 4 h, then every 6 h. One log line when it goes down, one when it is back. The banner and the provider card show it.
- **Downloads wait for an unreachable provider** instead of failing and being grabbed again. Radarr and Sonarr see them as queued. If the provider stays down, each fails once after **Settings → Downloads → When a provider is unreachable, give up after** (default 24 hours).
- **History:** new **Remove failed**, **Remove completed** and **Remove all** buttons. Files are never touched, and downloads Radarr or Sonarr haven't imported yet are kept.

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
