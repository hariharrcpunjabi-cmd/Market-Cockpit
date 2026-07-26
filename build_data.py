#!/usr/bin/env python3
"""
Market-Cockpit weekly data builder.
Runs server-side (GitHub Action) — no CORS, no proxy, no static IP needed.
Fetches Yahoo weekly closes, computes RS-blend sector ranking + market regime,
writes data.json (live) and history/<date>.json (accumulating snapshots).
"""

import json, time, math, os, datetime, urllib.request, urllib.error, urllib.parse

# ------------------------------------------------------------------ CONFIG
BENCH = {"key": "nifty", "name": "Nifty 50", "ticker": "^NSEI"}
VIX   = {"key": "vix",   "name": "India VIX", "ticker": "^INDIAVIX"}

# Proven direct Yahoo indices (these gave 14/14 on the live browser test)
SECTORS_DIRECT = [
    {"name": "IT",           "ticker": "^CNXIT"},
    {"name": "Bank",         "ticker": "^NSEBANK"},
    {"name": "Auto",         "ticker": "^CNXAUTO"},
    {"name": "Pharma",       "ticker": "^CNXPHARMA"},
    {"name": "FMCG",         "ticker": "^CNXFMCG"},
    {"name": "Metal",        "ticker": "^CNXMETAL"},
    {"name": "Realty",       "ticker": "^CNXREALTY"},
    {"name": "Energy",       "ticker": "^CNXENERGY"},
    {"name": "Media",        "ticker": "^CNXMEDIA"},
    {"name": "PSU Bank",     "ticker": "^CNXPSUBANK"},
    {"name": "Infra",        "ticker": "^CNXINFRA"},
    {"name": "Fin Services", "ticker": "NIFTY_FIN_SERVICE.NS"},
    {"name": "PSE",          "ticker": "^CNXPSE"},
]

# Experimental ETF proxies for indices Yahoo won't serve directly.
# The log/JSON reports which resolve — we promote the winners next round.
SECTORS_PROXY = [
    {"name": "Consumption", "ticker": "CONSUMBEES.NS", "proxy": True},
    {"name": "Pvt Bank",    "ticker": "PVTBANIETF.NS", "proxy": True},
    {"name": "Healthcare",  "ticker": "HEALTHIETF.NS", "proxy": True},
    {"name": "Oil & Gas",   "ticker": "OILIETF.NS",    "proxy": True},
]

# ---- Page 4: macro / cross-asset signals -------------------------------
# dir "off" = rising reading is risk-OFF; "on" = rising reading is risk-ON.
# a/b are Yahoo tickers; kind "ratio" uses a/b, "level" uses a only.
MACRO = [
    {"name": "Gold / Silver ratio", "kind": "ratio", "a": "GC=F", "b": "SI=F",
     "dir": "off", "w": 0.8, "show": "ratio",
     "up": "fear bid — risk-off tilt", "down": "risk appetite returning"},
    {"name": "Nifty vs Gold", "kind": "ratio", "a": "^NSEI", "b": "GOLDBEES.NS",
     "dir": "on", "w": 1.0, "show": "change",
     "up": "equities beating safe-haven — risk-on", "down": "gold outrunning equities — defensive"},
    {"name": "Breadth · Nifty 500 / Nifty 50", "kind": "ratio", "a": "^CRSLDX", "b": "^NSEI",
     "dir": "on", "w": 1.0, "show": "change",
     "up": "broad participation — healthy", "down": "narrowing leadership — caution"},
    {"name": "US 10Y yield", "kind": "level", "a": "^TNX", "b": None,
     "dir": "off", "w": 0.5, "show": "change",
     "up": "yields climbing — equity headwind", "down": "yields easing — tailwind"},
]

LOOKBACK = 26      # rolling window for z-scores (weeks)
MOM_ROC  = 4       # momentum measured over N weeks
W_LEVEL, W_MOM = 0.6, 0.4      # blend weights (leans steady)
OW_COUNT, AV_COUNT = 4, 4      # top/bottom tag counts

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/124.0 Safari/537.36"}
HOSTS = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]

# ------------------------------------------------------------------ FETCH
def fetch(ticker, retries=3):
    """Return list[(date_epoch, close)] weekly, or None."""
    path = f"/v8/finance/chart/{urllib.parse.quote(ticker)}?range=2y&interval=1wk"
    for attempt in range(retries):
        host = HOSTS[attempt % len(HOSTS)]
        url = f"https://{host}{path}"
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=25) as r:
                j = json.load(r)
            res = j.get("chart", {}).get("result")
            if not res:
                continue
            res = res[0]
            ts = res.get("timestamp")
            cl = res.get("indicators", {}).get("quote", [{}])[0].get("close")
            if not ts or not cl:
                continue
            series = [(ts[i], cl[i]) for i in range(len(ts)) if cl[i] is not None]
            if len(series) < LOOKBACK + MOM_ROC + 2:
                continue
            return series
        except Exception as e:
            time.sleep(1.2 * (attempt + 1))
    return None

# ------------------------------------------------------------------ MATH
def mean(a): return sum(a) / len(a)
def std(a):
    m = mean(a); return math.sqrt(mean([(x - m) ** 2 for x in a]))
def ema(arr, span):
    k = 2 / (span + 1); prev = arr[0]; out = [prev]
    for x in arr[1:]:
        prev = x * k + prev * (1 - k); out.append(prev)
    return out
def last_z(series):
    w = series[-LOOKBACK:]; s = std(w)
    return 0.0 if s == 0 else (series[-1] - mean(w)) / s

# ------------------------------------------------------------------ REGIME
def compute_regime(bench_series, vix_series):
    closes = [c for _, c in bench_series]
    ma = mean(closes[-30:]); last = closes[-1]
    pct = (last / ma - 1) * 100; up = last > ma

    vix_val, band = None, "n/a"
    if vix_series:
        vix_val = vix_series[-1][1]
        if   vix_val < 11: band = "very low"
        elif vix_val < 16: band = "calm"
        elif vix_val < 20: band = "elevated"
        elif vix_val < 29: band = "high"
        elif vix_val <= 30: band = "extreme"
        else: band = "crash-like"

    vix_calm = vix_val is None or vix_val < 20
    vix_hot  = vix_val is not None and vix_val >= 29
    if up and vix_calm and not vix_hot:
        cls, verdict = "strong", "STRONG"
        note = "Trust the ranking — overweight the top sectors."
        if vix_val is not None and vix_val < 11:
            note = "Trust the ranking, but sub-11 VIX means stay alert for profit-booking."
    elif (not up) or vix_hot:
        cls, verdict = "weak", "WEAK"
        note = ("Ranking still shows relative leadership, but size down or sit out — "
                "\"best of a falling field\" isn't a real long.")
    else:
        cls, verdict = "sideways", "SIDEWAYS"
        note = "Leadership is readable but choppy — favour the top 2-3, keep sizing modest."

    return {"cls": cls, "verdict": verdict, "note": note,
            "pct_vs_ma": round(pct, 2), "trend_up": up,
            "vix": round(vix_val, 2) if vix_val is not None else None, "vix_band": band}

# ------------------------------------------------------------------ MACRO SIGNALS
def signal_stats(series):
    """series = list[(t, value)] -> (trend_z, chg13_pct, arrow, last_value)."""
    vals = [v for _, v in sorted(series)]
    sm = ema(vals, 5)
    z = last_z(sm)
    k = 13
    chg = (sm[-1] / sm[-1 - k] - 1) * 100 if len(sm) > k else 0.0
    arrow = "up" if chg > 0.5 else "down" if chg < -0.5 else "flat"
    return z, chg, arrow, vals[-1]

def compute_macro():
    diag, signals, score_num, score_den = [], [], 0.0, 0.0
    cache = {}
    def get(tk):
        if tk not in cache:
            cache[tk] = fetch(tk)
        return cache[tk]

    for m in MACRO:
        a = get(m["a"])
        b = get(m["b"]) if m["b"] else True
        ok = a is not None and b is not None
        diag.append({"name": m["name"], "ok": ok})
        if not ok:
            continue
        if m["kind"] == "ratio":
            ma = {t: c for t, c in a}; mb = {t: c for t, c in b}
            ts = sorted(set(ma) & set(mb))
            series = [(t, ma[t] / mb[t]) for t in ts if mb[t]]
        else:
            series = a
        if len(series) < LOOKBACK + 15:
            diag[-1]["ok"] = False
            continue
        z, chg, arrow, val = signal_stats(series)
        reading = m["up"] if arrow == "up" else m["down"] if arrow == "down" else "flat — no clear tilt"
        signals.append({
            "name": m["name"], "show": m["show"], "arrow": arrow,
            "z": round(z, 2), "chg13": round(chg, 2),
            "value": round(val, 2), "reading": reading,
        })
        contrib = z if m["dir"] == "on" else -z    # risk-on-ness
        score_num += m["w"] * contrib; score_den += m["w"]

    score = score_num / score_den if score_den else 0.0
    if score > 0.4:
        cls, label = "on", "RISK-ON"
        note = "Cross-asset signals lean risk-on — the sector leaderboard is worth trusting."
    elif score < -0.4:
        cls, label = "off", "RISK-OFF"
        note = "Cross-asset signals lean risk-off — treat equity longs cautiously, respect the regime gate."
    else:
        cls, label = "neutral", "NEUTRAL"
        note = "Mixed cross-asset signals — no strong edge; lean on sector selection and stay nimble."

    return {"verdict": {"cls": cls, "label": label, "note": note, "score": round(score, 2)},
            "signals": signals,
            "diagnostics": {"loaded": sum(1 for d in diag if d["ok"]), "total": len(diag), "detail": diag}}

# ------------------------------------------------------------------ MAIN
def main():
    diag = []
    print("Fetching benchmark…")
    bench = fetch(BENCH["ticker"])
    diag.append({"name": BENCH["name"], "ok": bench is not None})
    if not bench:
        raise SystemExit("FATAL: benchmark (Nifty 50) unreachable from the runner.")
    bench_map = {t: c for t, c in bench}
    dates = [t for t, _ in bench]

    vix = fetch(VIX["ticker"])
    diag.append({"name": VIX["name"], "ok": vix is not None})

    results = []
    for s in SECTORS_DIRECT + SECTORS_PROXY:
        ser = fetch(s["ticker"])
        ok = ser is not None
        diag.append({"name": s["name"], "ticker": s["ticker"],
                     "ok": ok, "proxy": s.get("proxy", False)})
        print(f"  {s['name']:<14} {s['ticker']:<22} {'ok' if ok else 'FAIL'}")
        if not ok:
            continue
        m = {t: c for t, c in ser}
        rs, kept = [], []
        for d in dates:
            if d in m:
                rs.append(m[d] / bench_map[d]); kept.append(d)
        if len(rs) < LOOKBACK + MOM_ROC + 2:
            continue
        rs_s = ema(rs, 5)
        level_z = last_z(rs_s)
        roc = [rs_s[i] / rs_s[i - MOM_ROC] - 1 if i >= MOM_ROC else 0 for i in range(len(rs_s))]
        mom_z = last_z(roc[MOM_ROC:])
        blend = W_LEVEL * level_z + W_MOM * mom_z
        results.append({"name": s["name"], "proxy": s.get("proxy", False),
                        "level_z": round(level_z, 3), "mom_z": round(mom_z, 3),
                        "blend": round(blend, 3)})

    results.sort(key=lambda r: r["blend"], reverse=True)
    n = len(results)
    regime = compute_regime(bench, vix)

    # tags (gated by regime) + arrows
    for i, r in enumerate(results):
        r["rank"] = i + 1
        r["arrow"] = "up" if r["mom_z"] > 0.3 else "down" if r["mom_z"] < -0.3 else "flat"
        tag = "HOLDING"
        if i < OW_COUNT and r["blend"] > 0:
            tag = "WATCH" if regime["cls"] == "weak" else "OVERWEIGHT"
        elif i >= n - AV_COUNT and r["blend"] < 0:
            tag = "AVOID"
        r["tag"] = tag

    as_of = datetime.datetime.utcfromtimestamp(dates[-1]).strftime("%Y-%m-%d")
    payload = {
        "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": as_of,
        "regime": regime,
        "sectors": results,
        "diagnostics": {
            "loaded": sum(1 for d in diag if d["ok"]),
            "total": len(diag),
            "detail": diag,
        },
    }

    with open("data.json", "w") as f:
        json.dump(payload, f, indent=2)
    os.makedirs("history", exist_ok=True)
    with open(f"history/{as_of}.json", "w") as f:
        json.dump(payload, f, indent=2)

    # ---- Page 4: macro / cross-asset ----
    print("\nBuilding macro signals…")
    macro = compute_macro()
    macro["generated_at"] = payload["generated_at"]
    macro["as_of"] = as_of
    with open("macro.json", "w") as f:
        json.dump(macro, f, indent=2)
    with open(f"history/macro-{as_of}.json", "w") as f:
        json.dump(macro, f, indent=2)
    print(f"Macro verdict: {macro['verdict']['label']} (score {macro['verdict']['score']})  "
          f"signals={macro['diagnostics']['loaded']}/{macro['diagnostics']['total']}")

    print(f"\nDone. as_of={as_of}  sectors_ranked={n}  "
          f"loaded={payload['diagnostics']['loaded']}/{payload['diagnostics']['total']}")

if __name__ == "__main__":
    main()
