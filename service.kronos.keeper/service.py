#twentytw93-KronosTeam
from __future__ import annotations
import os
from datetime import datetime, date
import xbmc
import xbmcaddon
import xbmcgui
import xbmcvfs
import sqlite3

ADDON = xbmcaddon.Addon()
ADDON_ID = ADDON.getAddonInfo("id")
ADDON_NAME = ADDON.getAddonInfo("name") or "Kronos Keeper"

PROFILE_PATH = xbmcvfs.translatePath("special://profile/addon_data/{}/".format(ADDON_ID))
LAST_SCAN_PATH = os.path.join(PROFILE_PATH, "last_scan.txt")
LOCK_PATH = os.path.join(PROFILE_PATH, "scan.lock")

PROFILE_DB_DIR = xbmcvfs.translatePath("special://profile/Database/")
THUMBS_DIR = xbmcvfs.translatePath("special://thumbnails/")

SCAN_DAY = 5  # for testing (0=Mon ... 6=Sun)
LOCK_STALE_HOURS = 6
CHECK_INTERVAL_SECONDS_IDLE = 60
CHECK_INTERVAL_SECONDS_NOT_HOME = 120
BOOT_DELAY_SECONDS = 23

def log(msg, level=xbmc.LOGINFO):
    try:
        xbmc.log("[Kronos Keeper] {}".format(msg), level)
    except Exception:
        pass


def ensure_profile_dir():
    if not xbmcvfs.exists(PROFILE_PATH):
        xbmcvfs.mkdirs(PROFILE_PATH)


def read_text(path):
    if not xbmcvfs.exists(path):
        return None
    try:
        fh = xbmcvfs.File(path, "r")
        try:
            data = fh.read()
        finally:
            fh.close()
        return data
    except Exception:
        return None


def write_text(path, text):
    try:
        fh = xbmcvfs.File(path, "w")
        try:
            fh.write(text)
        finally:
            fh.close()
        return True
    except Exception:
        return False


def get_scan_interval():
    val = (ADDON.getSetting("scan_interval") or "").strip().lower()
    if val not in ("weekly", "monthly"):
        val = "weekly"
    return val


def get_last_scan_date():
    raw = read_text(LAST_SCAN_PATH)
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def set_last_scan_date(d: date):
    write_text(LAST_SCAN_PATH, d.isoformat())


def is_home_idle():
    try:
        return xbmc.getCondVisibility("Window.IsVisible(home)") and (not xbmc.Player().isPlaying())
    except Exception:
        return False


def should_scan_today(today: date, interval: str) -> bool:
    if today.weekday() != SCAN_DAY:
        return False
    last = get_last_scan_date()
    if last is None:
        return True
    delta = (today - last).days
    return delta >= (30 if interval == "monthly" else 7)


def lock_is_stale() -> bool:
    if not xbmcvfs.exists(LOCK_PATH):
        return False
    try:
        mtime = os.path.getmtime(LOCK_PATH)
        age = xbmc.Monitor().time() - mtime
        return age > (LOCK_STALE_HOURS * 3600)
    except Exception:
        return True


def acquire_lock() -> bool:
    if xbmcvfs.exists(LOCK_PATH):
        if lock_is_stale():
            log("Stale lock detected, clearing.")
            try:
                xbmcvfs.delete(LOCK_PATH)
            except Exception:
                pass
        else:
            return False
    return write_text(LOCK_PATH, datetime.now().isoformat())


def release_lock():
    try:
        if xbmcvfs.exists(LOCK_PATH):
            xbmcvfs.delete(LOCK_PATH)
    except Exception:
        pass


def list_profile_dbs():
    if not os.path.isdir(PROFILE_DB_DIR):
        return []
    try:
        names = os.listdir(PROFILE_DB_DIR)
    except Exception:
        return []
    targets = []
    for n in names:
        if not n.lower().endswith(".db"):
            continue
        if (
            n.startswith("Textures")
            or n.startswith("Addons")
            or n.startswith("MyVideos")
            or n.startswith("MyMusic")
        ):
            targets.append(os.path.join(PROFILE_DB_DIR, n))
    return targets


def sqlite_integrity_check(db_path: str) -> (bool, str):
    uri = "file:{}?mode=ro".format(db_path)
    try:
        conn = sqlite3.connect(uri, uri=True)
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA integrity_check;")
            row = cur.fetchone()
            if not row:
                return False, "no result"
            res = (row[0] or "").lower()
            if res == "ok":
                return True, "ok"
            return False, res
        finally:
            conn.close()
    except sqlite3.OperationalError as e:
        return False, "operational_error: {}".format(e)
    except Exception as e:
        return False, "error: {}".format(e)


def run_check_sqlite_integrity(result: dict, monitor: xbmc.Monitor, progress_cb):
    dbs = list_profile_dbs()
    ok_all = True
    failures = []
    total = max(len(dbs), 1)
    for idx, db in enumerate(dbs, 1):
        if monitor.abortRequested():
            break
        ok, msg = sqlite_integrity_check(db)
        progress_cb(int(10 + (idx * 30.0 / total)))
        if not ok:
            ok_all = False
            failures.append({"db": db, "error": msg})
            log("SQLite FAIL: {} :: {}".format(db, msg), xbmc.LOGERROR)
        else:
            log("SQLite OK: {}".format(db))
    result["sqlite"] = {"ok": ok_all, "failures": failures, "scanned": len(dbs)}
    return ok_all


def get_textures_paths():
    db = os.path.join(PROFILE_DB_DIR, "Textures13.db")
    if not os.path.exists(db):
        return []
    uri = "file:{}?mode=ro".format(db)
    try:
        conn = sqlite3.connect(uri, uri=True)
        try:
            cur = conn.cursor()
            try:
                cur.execute("SELECT cachedurl FROM texture")
                rows = cur.fetchall()
                return [r[0] for r in rows if r and r[0]]
            except sqlite3.OperationalError:
                return []
        finally:
            conn.close()
    except Exception as e:
        log("Textures DB read error: {}".format(e), xbmc.LOGERROR)
        return []


def run_check_thumbnails(result: dict, monitor: xbmc.Monitor, progress_cb):
    cached = get_textures_paths()
    if not cached:
        result["thumbnails"] = {"ok": True, "missing": 0, "zerobyte": 0, "checked": 0}
        progress_cb(80)
        return True

    total = max(len(cached), 1)
    missing = zerobyte = checked = 0

    for idx, rel in enumerate(cached, 1):
        if monitor.abortRequested():
            break
        path = os.path.join(THUMBS_DIR, rel)
        if not os.path.exists(path):
            missing += 1
        else:
            try:
                if os.path.getsize(path) == 0:
                    zerobyte += 1
            except Exception:
                zerobyte += 1
        checked += 1
        progress_cb(int(40 + (idx * 50.0 / total)))

    result["thumbnails"] = {"ok": True, "missing": missing, "zerobyte": zerobyte, "checked": checked}
    if missing or zerobyte:
        log("Thumbs: missing={}, zerobyte={}, checked={}".format(missing, zerobyte, checked))
    return True


def perform_corruption_scan(monitor: xbmc.Monitor) -> dict:
    dlg = xbmcgui.DialogProgressBG()
    dlg.create(ADDON_NAME, "Initializing…")

    def set_progress(pct, msg=None):
        try:
            dlg.update(int(max(0, min(100, pct))), msg or "")
        except Exception:
            pass

    result = {"ok": True}
    try:
        if monitor.abortRequested():
            return result
        set_progress(8, "Checking databases…")
        result["ok"] &= run_check_sqlite_integrity(result, monitor, set_progress)

        if monitor.abortRequested():
            return result
        set_progress(45, "Auditing thumbnails…")
        result["ok"] &= run_check_thumbnails(result, monitor, set_progress)

        set_progress(100, "Done")
        return result
    finally:
        try:
            dlg.close()
        except Exception:
            pass


def notify_result(success: bool, details: dict):
    if success:
        xbmcgui.Dialog().notification(ADDON_NAME, "Scan complete: No corruption detected", xbmcgui.NOTIFICATION_INFO, 4000)
    else:
        xbmcgui.Dialog().notification(ADDON_NAME, "Issues found: Check logs", xbmcgui.NOTIFICATION_ERROR, 6000)
    log("Summary: {}".format(details))


def main():
    ensure_profile_dir()
    mon = xbmc.Monitor()

    log("Boot detected. Waiting {}s…".format(BOOT_DELAY_SECONDS))
    if mon.waitForAbort(BOOT_DELAY_SECONDS):
        return

    log("Service started. Scheduler active.")
    while not mon.abortRequested():
        today = date.today()
        interval = get_scan_interval()
        want_scan = should_scan_today(today, interval)

        if want_scan and is_home_idle():
            log("Conditions met. Trying lock.")
            if acquire_lock():
                try:
                    log("Starting scan…")
                    details = perform_corruption_scan(mon)
                    ok = bool(details.get("ok", True))
                    notify_result(ok, details)
                    set_last_scan_date(date.today())
                    log("Scan finished. Last scan date updated.")
                finally:
                    release_lock()
            else:
                log("Lock not acquired (already scanning?).")

        if xbmc.getCondVisibility("Window.IsVisible(home)"):
            if mon.waitForAbort(CHECK_INTERVAL_SECONDS_IDLE):
                return
        else:
            if mon.waitForAbort(CHECK_INTERVAL_SECONDS_NOT_HOME):
                return


if __name__ == "__main__":
    main()