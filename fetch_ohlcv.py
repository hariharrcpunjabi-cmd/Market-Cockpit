#!/usr/bin/env python3
"""
fetch_ohlcv.py — builds and maintains a local daily OHLCV cache for the
Market Cockpit, using the same raw Yahoo chart endpoint build_data.py already
uses. Standard library only: no yfinance, no pandas, nothing to pip install.

WHY THIS EXISTS
---------------
Chartink gives one row per stock per week and overwrites it. That is enough to
rank today's list and nothing else. A real backtest, the historical breadth
study, a home-grown valuation/cycle index, and charts on cards all need price
history. This script is that history.

STORAGE
-------
    ohlcv/2015.csv.gz ... ohlcv/2026.csv.gz     one file per calendar year
    ohlcv/_state.json                            progress + last-date index

Partitioning by year is deliberate. Git stores a full copy of every version of
every changed file, so rewriting one large cache weekly would inflate the repo
without limit. Only the current year's file changes on a normal run.

USAGE
-----
    python fetch_ohlcv.py --backfill            # first run, ~10y, resumable
    python fetch_ohlcv.py --backfill --limit 50 # smoke test on 50 symbols
    python fetch_ohlcv.py                       # weekly incremental
    python fetch_ohlcv.py --verify              # report coverage, fetch nothing

The backfill is slow (roughly 25-45 min for ~1200 symbols) and is designed to
be interrupted. Re-running resumes from _state.json rather than starting over,
so a timed-out Action loses nothing.
"""

import csv, gzip, json, os, sys, time, datetime, urllib.request, urllib.parse

# ----------------------------------------------------------------- config
OUT_DIR      = "ohlcv"
STATE_PATH   = os.path.join(OUT_DIR, "_state.json")
HOSTS        = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]
HEADERS      = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/124.0 Safari/537.36"}
SLEEP        = 0.9      # polite gap between symbols
MAX_RETRIES  = 3
BACKFILL_RNG = "10y"
FIELDS       = ["symbol", "date", "open", "high", "low", "close", "volume"]

# Indices worth caching alongside the stocks — the composite index and any
# relative-strength work needs these as denominators.
INDEX_TICKERS = {
    "^NSEI": "NIFTY50", "^CNX100": "NIFTY100", "^CRSLDX": "NIFTY500",
    "^NSEBANK": "BANKNIFTY", "^INDIAVIX": "INDIAVIX",
    "NIFTY_MIDCAP_100.NS": "NIFTYMIDCAP100",
}


# ----------------------------------------------------------------- universe
def load_universe(csv_path=None, limit=None):
    """Symbols to cache: the Holdings scan (widest net we export) plus indices.

    Holdings is the right source because it is the least filtered scan — close
    above the 150-MA and a market-cap floor. Using Trading instead would cache
    only names that happen to qualify today, which defeats the purpose.
    """
    syms = []
    if csv_path is None:
        for cand in os.listdir("."):
            if cand.lower().endswith(".csv") and "holding" in cand.lower():
                csv_path = cand
                break
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                s = (row.get("Symbol") or row.get("symbol") or "").strip()
                if s:
                    syms.append(s)
    else:
        print("  ! no holdings CSV found — caching indices only")

    seen, uni = set(), []
    for s in syms:
        if s not in seen:
            seen.add(s)
            uni.append((f"{s}.NS", s))          # Yahoo wants the .NS suffix
    if limit:
        uni = uni[:limit]
    for tk, name in INDEX_TICKERS.items():      # indices always included
        if name not in seen:
            uni.append((tk, name))
    return uni


# ----------------------------------------------------------------- fetch
def fetch_bars(ticker, rng="1mo", retries=MAX_RETRIES):
    """Return list of (date_iso, o, h, l, c, v) or None. Daily interval."""
    path = (f"/v8/finance/chart/{urllib.parse.quote(ticker)}"
            f"?range={rng}&interval=1d")
    for attempt in range(retries):
        host = HOSTS[attempt % len(HOSTS)]
        try:
            req = urllib.request.Request(f"https://{host}{path}", headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                j = json.load(r)
            res = (j.get("chart") or {}).get("result")
            if not res:
                return None
            res = res[0]
            ts = res.get("timestamp") or []
            q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
            o, h, l, c, v = (q.get("open"), q.get("high"), q.get("low"),
                             q.get("close"), q.get("volume"))
            if not ts or c is None:
                return None
            out = []
            for i in range(len(ts)):
                if c[i] is None:                # holidays / halts
                    continue
                d = datetime.datetime.utcfromtimestamp(ts[i]).date().isoformat()
                out.append((d,
                            _r(o[i]), _r(h[i]), _r(l[i]), _r(c[i]),
                            int(v[i]) if v and v[i] is not None else 0))
            return out
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(1.5 * (attempt + 1))
    return None


def _r(x):
    return round(float(x), 2) if x is not None else None


# ----------------------------------------------------------------- storage
def year_path(y):
    return os.path.join(OUT_DIR, f"{y}.csv.gz")


def read_year(y):
    """-> {(symbol, date): row}"""
    p, rows = year_path(y), {}
    if not os.path.exists(p):
        return rows
    with gzip.open(p, "rt", newline="") as f:
        for r in csv.DictReader(f):
            rows[(r["symbol"], r["date"])] = r
    return rows


def write_year(y, rows):
    os.makedirs(OUT_DIR, exist_ok=True)
    ordered = sorted(rows.values(), key=lambda r: (r["symbol"], r["date"]))
    with gzip.open(year_path(y), "wt", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(ordered)


def merge_bars(symbol, bars):
    """Split bars by year and merge into the year files. Returns rows added."""
    by_year = {}
    for d, o, h, l, c, v in bars:
        by_year.setdefault(d[:4], []).append(
            {"symbol": symbol, "date": d, "open": o, "high": h,
             "low": l, "close": c, "volume": v})
    added = 0
    for y, rows in by_year.items():
        existing = read_year(y)
        before = len(existing)
        for r in rows:
            existing[(r["symbol"], r["date"])] = r   # last write wins
        added += len(existing) - before
        write_year(y, existing)
    return added


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            return json.load(open(STATE_PATH))
        except Exception:
            pass
    return {"last_date": {}, "done_backfill": [], "updated_at": None}


def save_state(st):
    os.makedirs(OUT_DIR, exist_ok=True)
    st["updated_at"] = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    json.dump(st, open(STATE_PATH, "w"), indent=1, sort_keys=True)


# ----------------------------------------------------------------- runs
def run(backfill=False, limit=None, verify=False):
    os.makedirs(OUT_DIR, exist_ok=True)
    uni = load_universe(limit=limit)
    st = load_state()
    print(f"universe: {len(uni)} tickers | mode: "
          f"{'verify' if verify else ('backfill' if backfill else 'incremental')}")

    if verify:
        return report(st, uni)

    done = set(st.get("done_backfill", []))
    ok = fail = added_total = 0
    t0 = time.time()

    for i, (ticker, name) in enumerate(uni, 1):
        if backfill and name in done:
            continue

        rng = BACKFILL_RNG if backfill else _incremental_range(st, name)
        if rng is None:                       # already current
            continue

        bars = fetch_bars(ticker, rng=rng)
        if not bars:
            fail += 1
            print(f"  [{i}/{len(uni)}] {name:<14} FAIL")
            time.sleep(SLEEP)
            continue

        added_total += merge_bars(name, bars)
        st["last_date"][name] = bars[-1][0]
        if backfill:
            done.add(name)
            st["done_backfill"] = sorted(done)
        ok += 1

        if i % 25 == 0 or i == len(uni):      # checkpoint so a timeout is safe
            save_state(st)
            el = time.time() - t0
            print(f"  [{i}/{len(uni)}] ok={ok} fail={fail} "
                  f"rows+={added_total} {el/60:.1f}min")
        time.sleep(SLEEP)

    save_state(st)
    print(f"\ndone: {ok} ok, {fail} failed, {added_total} rows added, "
          f"{(time.time()-t0)/60:.1f} min")
    report(st, uni)
    return 0 if ok else 1


def _incremental_range(st, name):
    """Smallest Yahoo range that covers the gap since we last saw this symbol."""
    last = st.get("last_date", {}).get(name)
    if not last:
        return BACKFILL_RNG                    # never seen: full history
    try:
        gap = (datetime.date.today() - datetime.date.fromisoformat(last)).days
    except Exception:
        return "1y"
    if gap <= 0:
        return None
    if gap <= 5:
        return "5d"
    if gap <= 25:
        return "1mo"
    if gap <= 80:
        return "3mo"
    if gap <= 300:
        return "1y"
    return BACKFILL_RNG


def report(st, uni):
    years = sorted(f[:-7] for f in os.listdir(OUT_DIR)
                   if f.endswith(".csv.gz")) if os.path.isdir(OUT_DIR) else []
    total_rows = total_bytes = 0
    print("\ncoverage:")
    for y in years:
        n = len(read_year(y))
        b = os.path.getsize(year_path(y))
        total_rows += n
        total_bytes += b
        print(f"  {y}: {n:>9,} rows  {b/1e6:>6.1f} MB")
    have = len(st.get("last_date", {}))
    print(f"  TOTAL: {total_rows:,} rows  {total_bytes/1e6:.1f} MB  "
          f"| {have}/{len(uni)} symbols have data")
    stale = [k for k, v in st.get("last_date", {}).items()
             if (datetime.date.today() - datetime.date.fromisoformat(v)).days > 10]
    if stale:
        print(f"  ! {len(stale)} symbols not updated in >10 days: "
              f"{', '.join(stale[:8])}{' ...' if len(stale) > 8 else ''}")
    return 0


if __name__ == "__main__":
    a = sys.argv[1:]
    lim = None
    if "--limit" in a:
        lim = int(a[a.index("--limit") + 1])
    sys.exit(run(backfill="--backfill" in a, limit=lim, verify="--verify" in a))
