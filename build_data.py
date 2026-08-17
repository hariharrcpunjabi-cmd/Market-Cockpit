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
# show: how to display the headline number.
MACRO = [
    {"name": "Gold vs Silver", "kind": "ratio", "a": "GC=F", "b": "SI=F",
     "dir": "off", "w": 0.8, "show": "ratio",
     "why": "The gold-to-silver ratio climbs when investors turn defensive.",
     "up": "Gold is pulling ahead of silver — a classic fear signal. Mild caution for stocks.",
     "down": "Silver is catching up to gold — risk appetite returning. Supportive for stocks.",
     "flat": "Gold and silver moving together — no clear signal."},
    {"name": "Nifty 50 vs Gold", "kind": "ratio", "a": "^NSEI", "b": "GOLDBEES.NS",
     "dir": "on", "w": 1.0, "show": "change",
     "why": "Large-caps vs gold — when stocks outrun gold, risk appetite is healthy.",
     "up": "Large-cap stocks are beating gold — money prefers equities over safe-havens. Good sign.",
     "down": "Gold is beating large-cap stocks — money is hiding in safe-havens. Defensive sign.",
     "flat": "Large-caps and gold roughly even — no clear preference."},
    {"name": "Nifty 500 vs Gold", "kind": "ratio", "a": "^CRSLDX", "b": "GOLDBEES.NS",
     "dir": "on", "w": 0.7, "show": "change",
     "why": "Whole market vs gold — broad risk appetite across all caps.",
     "up": "The broad market is beating gold — risk appetite is broad-based. Good sign.",
     "down": "Gold is beating the broad market — defensive across the board.",
     "flat": "Broad market and gold roughly even."},
    {"name": "Market Breadth", "kind": "ratio", "a": "^CRSLDX", "b": "^NSEI",
     "dir": "on", "w": 1.0, "show": "change",
     "why": "Nifty 500 vs Nifty 50 — broad participation signals a durable move.",
     "up": "The broader market is keeping pace with large-caps — healthy, broad participation.",
     "down": "Only large-caps are holding up while the broader market lags — narrow and fragile.",
     "flat": "Broad market and large-caps moving together — steady."},
    {"name": "US 10-Year Yield", "kind": "level", "a": "^TNX", "b": None,
     "dir": "off", "w": 0.5, "show": "yield",
     "why": "High US yields pull foreign money out of emerging markets like India.",
     "up": "US bond yields are rising — global money gets pricier, a headwind for Indian equities.",
     "down": "US bond yields are easing — cheaper global money, a tailwind for Indian equities.",
     "flat": "US yields steady — neutral backdrop."},
]

LOOKBACK = 26      # rolling window for z-scores (weeks)
MOM_ROC  = 4       # momentum measured over N weeks
W_LEVEL, W_MOM = 0.6, 0.4      # blend weights (leans steady)
OW_COUNT, AV_COUNT = 4, 4      # top/bottom tag counts

# ---- Page 1: native Stage 2 stock scan (Nifty 200 universe) ------------
# Weinstein Stage 2 conditions, computed from raw Yahoo daily data:
#   weekly RSI(14) >= 50 · daily close > daily SMA30 · daily vol > 100k
#   daily vol >= daily SMA50(vol) · weekly 30-MA rising · weekly close > weekly 30-MA
STAGE2_TOP = 20        # how many to publish
STAGE2_SLEEP = 0.15    # polite delay between stock fetches (throttle guard)

NIFTY200 = [
    # Nifty 50
    "RELIANCE","TCS","HDFCBANK","ICICIBANK","INFY","BHARTIARTL","SBIN","LICI","ITC",
    "HINDUNILVR","LT","BAJFINANCE","HCLTECH","KOTAKBANK","SUNPHARMA","MARUTI","AXISBANK",
    "M&M","NTPC","ULTRACEMCO","TITAN","ADANIENT","ONGC","TATAMOTORS","POWERGRID","ASIANPAINT",
    "COALINDIA","BAJAJFINSV","WIPRO","NESTLEIND","JSWSTEEL","ADANIPORTS","TATASTEEL","GRASIM",
    "HDFCLIFE","SBILIFE","TECHM","BAJAJ-AUTO","HINDALCO","BRITANNIA","DRREDDY","CIPLA",
    "EICHERMOT","APOLLOHOSP","INDUSINDBK","TATACONSUM","BPCL","DIVISLAB","HEROMOTOCO","SHRIRAMFIN",
    # Next 50 + large/mid
    "ADANIGREEN","ADANIPOWER","AMBUJACEM","DLF","BANKBARODA","GAIL","VEDL","PNB","IOC",
    "SIEMENS","PIDILITIND","GODREJCP","HAVELLS","DABUR","MARICO","BEL","BOSCHLTD","TRENT",
    "ZOMATO","JINDALSTEL","VBL","CHOLAFIN","TVSMOTOR","ICICIPRULI","ICICIGI","SBICARD",
    "COLPAL","BERGEPAINT","MCDOWELL-N","TORNTPHARM","NAUKRI","ZYDUSLIFE","LUPIN","AUROPHARMA",
    "INDIGO","CGPOWER","HAL","MAZDOCK","BHEL","IRCTC","IRFC","PFC","RECLTD","MOTHERSON",
    "ABB","POLYCAB","SRF","PAGEIND","MUTHOOTFIN","BAJAJHLDNG","LTIM","PERSISTENT","MPHASIS",
    "COFORGE","OFSS","TATAPOWER","NHPC","JSWENERGY","TATAELXSI","DMART","PGHH","UNITDSPR",
    "BALKRISIND","MRF","ASHOKLEY","BHARATFORG","CUMMINSIND","ABBOTINDIA","ALKEM","BIOCON",
    "GLENMARK","IPCALAB","LAURUSLABS","PEL","SYNGENE","GLAND","MANKIND","PATANJALI","FORTIS",
    "MAXHEALTH","LALPATHLAB","METROPOLIS","JUBLFOOD","TATACOMM","INDUSTOWER","IDEA","HONAUT",
    "3MINDIA","GRINDWELL","THERMAX","AIAENG","SUPREMEIND","ASTRAL","KANSAINER","APLAPOLLO",
    "JINDALSAW","SAIL","NMDC","NATIONALUM","HINDCOPPER","RATNAMANI","CONCOR","GMRINFRA",
    "IGL","MGL","GUJGASLTD","PETRONET","OIL","MRPL","CASTROLIND","AARTIIND","DEEPAKNTR",
    "TATACHEM","PIIND","UPL","COROMANDEL","CHAMBLFERT","BAYERCROP","SUMICHEM","LINDEINDIA",
    "BANDHANBNK","FEDERALBNK","IDFCFIRSTB","AUBANK","BANKINDIA","UNIONBANK","CANBK","INDIANB",
    "YESBANK","RBLBANK","MANAPPURAM","LICHSGFIN","M&MFIN","POONAWALLA","IIFL","ANGELONE",
    "BSE","CDSL","CAMS","KFINTECH","360ONE","JIOFIN","POLICYBZR","PAYTM","DELHIVERY",
    "NYKAA","IRB","KPITTECH","LTTS","CYIENT","SONACOMS","UNOMINDA","EXIDEIND","ENDURANCE",
    "SCHAEFFLER","TIINDIA","ESCORTS","BALRAMCHIN","DALBHARAT","JKCEMENT","RAMCOCEM","ACC",
    "INDHOTEL","PHOENIXLTD","OBEROIRLTY","GODREJPROP","PRESTIGE","LODHA","BRIGADE","NBCC",
    "SJVN","TATAINVEST","SUNDARMFIN","CROMPTON","VOLTAS","BLUESTARCO","DIXON","AMBER",
    "KEI","FINCABLES","STARHEALTH","NIACL","GICRE","UBL","RADICO","GODFRYPHLP",
]
NIFTY200 = [s + ".NS" for s in NIFTY200]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/124.0 Safari/537.36"}
HOSTS = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]

# ------------------------------------------------------------------ FETCH
def fetch(ticker, retries=3, rng="2y"):
    """Return list[(date_epoch, close)] weekly, or None."""
    path = f"/v8/finance/chart/{urllib.parse.quote(ticker)}?range={rng}&interval=1wk"
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

def _percentile(vals, current):
    if not vals: return 50.0
    return sum(1 for v in vals if v <= current) / len(vals) * 100

def compute_macro():
    diag, signals, score_num, score_den = [], [], 0.0, 0.0
    cache = {}
    def get(tk):
        if tk not in cache:
            cache[tk] = fetch(tk, rng="5y")   # 5 years for percentile context
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
        sm = ema([v for _, v in sorted(series)], 5)

        # US 10Y scale fix: Yahoo ^TNX sometimes quotes yield ×10
        if m["show"] == "yield" and val > 20:
            val = val / 10

        # 5-year percentile of current reading
        pct = _percentile(sm, sm[-1])
        if   pct >= 85: pos = "near the top of"
        elif pct >= 65: pos = "in the upper part of"
        elif pct <= 15: pos = "near the bottom of"
        elif pct <= 35: pos = "in the lower part of"
        else:           pos = "mid-range within"
        context = f"At the {pct:.0f}th percentile — {pos} its 5-year range."

        if m["show"] == "ratio":   headline = f"{val:.1f}"
        elif m["show"] == "yield": headline = f"{val:.2f}%"
        else:                      headline = f"{chg:+.1f}%"

        # percentile-based risk read (auto-calibrated, per user's choice)
        ron = (pct - 50) / 50 if m["dir"] == "on" else -(pct - 50) / 50
        status = "good" if ron > 0.3 else "bad" if ron < -0.3 else "mid"
        plain = m["up"] if arrow == "up" else m["down"] if arrow == "down" else m["flat"]

        signals.append({
            "name": m["name"], "headline": headline, "arrow": arrow, "status": status,
            "plain": plain, "why": m["why"], "context": context,
            "percentile": round(pct), "chg13": round(chg, 1), "z": round(z, 2),
        })
        score_num += m["w"] * ron; score_den += m["w"]

    score = score_num / score_den if score_den else 0.0
    if score > 0.4:
        cls, label = "on", "RISK-ON"
        action = "Macro backdrop is supportive. You can lean into the top of the sector leaderboard with normal sizing."
    elif score < -0.4:
        cls, label = "off", "RISK-OFF"
        action = "Macro backdrop is defensive. Go easy on new equity longs — favour only the strongest sectors and keep sizing small, or wait."
    else:
        cls, label = "neutral", "MIXED"
        action = "No clear macro edge. Stay invested but selective — let the sector leaderboard pick your spots and keep sizing normal. Don't force new risk."

    return {"verdict": {"cls": cls, "label": label, "action": action, "score": round(score, 2)},
            "signals": signals,
            "diagnostics": {"loaded": sum(1 for d in diag if d["ok"]), "total": len(diag), "detail": diag}}

# ------------------------------------------------------------------ STAGE 2 SCAN
def fetch_ohlcv(ticker, rng="1y", interval="1d", retries=3):
    """Return list[(epoch, close, volume)] or None."""
    path = f"/v8/finance/chart/{urllib.parse.quote(ticker)}?range={rng}&interval={interval}"
    for attempt in range(retries):
        host = HOSTS[attempt % len(HOSTS)]
        try:
            req = urllib.request.Request(f"https://{host}{path}", headers=HEADERS)
            with urllib.request.urlopen(req, timeout=25) as r:
                j = json.load(r)
            res = j.get("chart", {}).get("result")
            if not res:
                continue
            res = res[0]; ts = res.get("timestamp")
            q = res.get("indicators", {}).get("quote", [{}])[0]
            cl, vol = q.get("close"), q.get("volume")
            hi, lo = q.get("high"), q.get("low")
            if not ts or not cl:
                continue
            out = [(ts[i], (hi[i] if hi and hi[i] is not None else cl[i]),
                    (lo[i] if lo and lo[i] is not None else cl[i]),
                    cl[i], (vol[i] if vol else None))
                   for i in range(len(ts)) if cl[i] is not None]
            return out or None
        except Exception:
            time.sleep(0.8 * (attempt + 1))
    return None

def sma(vals, n):
    return sum(vals[-n:]) / n if len(vals) >= n else None

def rsi_wilder(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return 100 - 100 / (1 + rs)

def resample_weekly_close(daily):
    """daily = list[(t, high, low, close, vol)] chronological -> list of weekly closes."""
    weeks = {}
    order = []
    for t, _h, _l, c, _v in daily:
        key = datetime.datetime.utcfromtimestamp(t).isocalendar()[:2]
        if key not in weeks:
            order.append(key)
        weeks[key] = c
    return [weeks[k] for k in order]

def atr14(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i-1]),
                       abs(lows[i] - closes[i-1])))
    return sum(trs[-period:]) / period

def pct_return(closes, days):
    if not closes or len(closes) <= days:
        return 0.0
    return (closes[-1] / closes[-1 - days] - 1) * 100

def scan_stage2():
    print("Scanning Nifty 200 for Stage 2…")
    nd = fetch_ohlcv("^NSEI", "1y", "1d")
    nifty_closes = [c for _, _, _, c, _ in nd] if nd else None
    nifty_ret = pct_return(nifty_closes, 65) if nifty_closes else 0.0

    passed, scanned, failed = [], 0, 0
    grade_count = {"Prime": 0, "Healthy": 0, "Late": 0}
    for tk in NIFTY200:
        d = fetch_ohlcv(tk, "1y", "1d")
        time.sleep(STAGE2_SLEEP)
        if not d or len(d) < 160:
            failed += 1; continue
        scanned += 1
        highs  = [h for _, h, _, _, _ in d]
        lows   = [l for _, _, l, _, _ in d]
        closes = [c for _, _, _, c, _ in d]
        vols   = [v for _, _, _, _, v in d if v is not None]
        wk = resample_weekly_close(d)
        dSMA30 = sma(closes, 30); dVolSMA50 = sma(vols, 50)
        if dSMA30 is None or dVolSMA50 is None or len(wk) < 32:
            continue
        dClose = closes[-1]; dVol = vols[-1] if vols else 0
        wClose = wk[-1]; wSMA30 = sma(wk, 30); wSMA30_prev = sma(wk[:-1], 30)
        wRSI = rsi_wilder(wk, 14)
        if None in (wSMA30, wSMA30_prev, wRSI):
            continue
        # six Stage 2 conditions
        if not (wRSI >= 50 and dClose > dSMA30 and dVol > 100000
                and dVol >= dVolSMA50 and wSMA30 >= wSMA30_prev and wClose > wSMA30):
            continue
        ext = (wClose / wSMA30 - 1) * 100
        vol_surge = dVol / dVolSMA50 if dVolSMA50 else 1.0
        rs13 = pct_return(closes, 65) - nifty_ret

        # ---- proper trading stop: tighter of (2x ATR) and (8% below) ----
        a = atr14(highs, lows, closes, 14)
        atr_stop = dClose - 2 * a if a else None
        pct_stop = dClose * 0.92
        stop = max(atr_stop, pct_stop) if atr_stop else pct_stop   # tighter = closer to price
        risk_pct = (dClose - stop) / dClose * 100

        # ---- Prime / Healthy / Late grade by extension above 30-wk MA ----
        grade = "Prime" if ext <= 8 else "Healthy" if ext <= 20 else "Late"
        grade_count[grade] += 1

        penalty = 0 if ext <= 25 else (ext - 25) * 0.5
        score = rs13 * 1.0 + min(vol_surge, 3) * 3.0 - penalty
        passed.append({
            "symbol": tk.replace(".NS", ""),
            "close": round(dClose, 2),
            "grade": grade,
            "ext_above_30wma": round(ext, 1),
            "vol_surge": round(vol_surge, 2),
            "rs_vs_nifty": round(rs13, 1),
            "atr": round(a, 2) if a else None,
            "stop": round(stop, 2),            # trading stop-loss (ATR/% mix)
            "risk_pct": round(risk_pct, 1),    # % risk from close to stop
            "stage2_exit": round(wSMA30, 2),   # structural: weekly close below 30-wk MA
            "score": round(score, 2),
        })
    passed.sort(key=lambda x: x["score"], reverse=True)
    print(f"  scanned={scanned} failed={failed} qualified={len(passed)} "
          f"(Prime {grade_count['Prime']} / Healthy {grade_count['Healthy']} / Late {grade_count['Late']})")
    return {"universe": "Nifty 200", "scanned": scanned, "failed": failed,
            "qualified": len(passed), "grades": grade_count,
            "stocks": passed[:STAGE2_TOP]}

# ------------------------------------------------------------------ CHARTINK CSV
CHARTINK_CSV = "chartink_trading.csv"   # committed to repo (manually for now, bridge later)

def _cnum(s):
    if s is None: return None
    s = str(s).replace(',', '').replace('%', '').strip().strip('"')
    try: return float(s)
    except: return None

def _tier(mcap_cr):
    if mcap_cr is None: return "—"
    if mcap_cr >= 20000: return "Large"
    if mcap_cr >= 5000:  return "Mid"
    if mcap_cr >= 500:   return "Small"
    return "Micro"

def _grade(ext):
    if ext < 0: return "Late"          # below the 150-MA — not a clean entry, avoid
    return "Prime" if ext <= 10 else "Healthy" if ext <= 20 else "Late"

def _sec_norm(s):
    if not s: return ""
    s = s.strip().title()
    return {"Fmcg": "FMCG", "It": "IT", "Nbfc": "NBFC"}.get(s, s)

def _find(row, *needles):
    """Find a CSV value by fuzzy header match (case/space-insensitive contains-all)."""
    for k, v in row.items():
        kl = (k or "").lower().replace(" ", "")
        if all(n in kl for n in needles):
            return v
    return None

# ---- Holdings scan: silent plumbing, never displayed --------------------
# Chartink scan "Claude_Holdings_Scan" = Close >= SMA150 AND MktCap >= 100.
# Its only job is to answer "is this old pick still above its 150-MA?" so the
# ledger doesn't falsely close a name that merely left the Trading buy list.
HOLDINGS_KEYWORDS = ["holding", "holdings_scan", "above150", "above_150"]

def parse_holdings_csv(path):
    """Light parse — we only need symbol / close / sma150."""
    import csv
    roster = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            sym = (r.get('Symbol') or r.get('symbol') or '').strip()
            close = _cnum(r.get('close')); s150 = _cnum(r.get('sma150'))
            if not sym or close is None:
                continue
            roster[sym] = {"close": round(close, 2),
                           "sma150": round(s150, 2) if s150 is not None else None}
    return roster

# scanner routing — matched against the CSV filename (lowercased)
SCANNERS = [
    {"id": "trading", "keywords": ["trading"], "label": "Trading", "style": "buy",
     "personal": False, "note": "Buy list — Stage 2 names near support."},
    {"id": "momentum", "keywords": ["hariharr", "momentum"], "label": "Momentum", "style": "buy",
     "personal": False, "note": "Strongest active trend right now. Aggressive — a Late-grade name here is a chase, not a buy."},
]

def parse_chartink_csv(path):
    import csv
    rows, gc, tc, sc = [], {"Prime":0,"Healthy":0,"Late":0}, {"Large":0,"Mid":0,"Small":0,"Micro":0,"—":0}, {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            sym = (r.get('Symbol') or '').strip()
            close = _cnum(r.get('close')); sma150 = _cnum(r.get('sma150'))
            atr = _cnum(r.get('atr14')); mcap = _cnum(r.get('market_cap'))
            if not sym or not close or not sma150: continue
            ext = (close / sma150 - 1) * 100
            g = _grade(ext); t = _tier(mcap); sec = _sec_norm(r.get('Sector') or r.get('sector'))
            ema21 = _cnum(_find(r, "ema", "21"))
            # weeks since 150-MA reclaim (from optional Chartink column: days above SMA150)
            days_above = _cnum(_find(r, "days", "above")) or _cnum(_find(r, "above", "150")) or _cnum(_find(r, "150", "count"))
            reclaim = None
            if days_above is not None:
                wks = days_above / 5.0   # trading days -> weeks
                reclaim = ("fresh" if wks <= 6 else "established" if wks <= 26 else "extended")
            # three stops, reader picks
            stop_ma = sma150 * 0.97
            risk_ma = (close - stop_ma) / close * 100
            stop_atr = (close - 2*atr) if atr else None
            risk_atr = (close - stop_atr) / close * 100 if stop_atr else None
            stop_ema = ema21 if (ema21 and ema21 < close) else None
            risk_ema = (close - stop_ema) / close * 100 if stop_ema else None
            # suggested stop by grade: Prime→MA-line, Healthy→ATR, Late→21-EMA (tight trail)
            if g == "Prime":     suggested = "ma"
            elif g == "Healthy": suggested = "atr" if stop_atr else "ma"
            else:                suggested = "ema" if stop_ema else ("atr" if stop_atr else "ma")
            sugg_stop = {"ma": stop_ma, "atr": stop_atr, "ema": stop_ema}.get(suggested) or stop_ma
            # optional HVE/HVA volume-footprint flags (Accumulation columns, if present)
            hva = _find(r, "hva"); hve = _find(r, "hve")
            hva = str(hva).strip().lower() in ("1", "true", "yes", "y") if hva is not None else False
            hve = str(hve).strip().lower() in ("1", "true", "yes", "y") if hve is not None else False
            gc[g] += 1; tc[t] += 1
            if sec: sc[sec] = sc.get(sec, 0) + 1
            rows.append({
                "symbol": sym, "name": (r.get('Stock Name') or '').strip(),
                "close": round(close, 2), "pct_change": _cnum(r.get('%_change')),
                "grade": g, "ext_above_30wma": round(ext, 1),
                "mcap_cr": round(mcap) if mcap else None, "tier": t, "sector": sec,
                "stop_ma": round(stop_ma, 2), "risk_ma": round(risk_ma, 1),
                "stop_atr": round(stop_atr, 2) if stop_atr else None,
                "risk_atr": round(risk_atr, 1) if risk_atr else None,
                "stop_ema": round(stop_ema, 2) if stop_ema else None,
                "risk_ema": round(risk_ema, 1) if risk_ema else None,
                "suggested": suggested, "sugg_stop": round(sugg_stop, 2),
                "hva": hva, "hve": hve,
                "days_above_150": int(days_above) if days_above is not None else None,
                "reclaim": reclaim,
                "stage2_exit": round(sma150, 2),
            })
    grank = {"Prime":0,"Healthy":1,"Late":2}
    rmap = {"fresh":0, "established":1, "extended":2, None:1}
    rows.sort(key=lambda x: (grank[x["grade"]], rmap.get(x["reclaim"],1),
                             x["risk_ma"] if x["suggested"]=="ma" else (x["risk_atr"] or 99),
                             x["ext_above_30wma"]))
    for i, x in enumerate(rows): x["rank"] = i + 1
    return {"qualified": len(rows), "grades": gc, "tiers": tc,
            "sector_mix": sc, "stocks": rows}

# ------------------------------------------------------------------ TRACKER
def compute_tracker(current_stocks, as_of):
    """Aging + new/dropped + %return + SL-hit, from committed weekly snapshots."""
    import glob
    timeline = []   # (date, {symbol: close})
    for sp in sorted(glob.glob("history/stage2-*.json")):
        try:
            d = json.load(open(sp))
            date = d.get("as_of") or sp.split("stage2-")[-1].replace(".json", "")
            if "scanners" in d:
                stocks = d["scanners"].get("trading", {}).get("stocks", [])
            else:
                stocks = d.get("stocks", [])
            smap = {s["symbol"]: s.get("close") for s in (stocks or []) if s.get("symbol")}
            timeline.append((date, smap))
        except Exception:
            pass
    timeline.sort()
    prev_syms = set(timeline[-2][1].keys()) if len(timeline) >= 2 else set()

    cur = {s["symbol"]: s for s in current_stocks if s.get("symbol")}
    out = []
    for sym, s in cur.items():
        weeks, first_close = 0, s.get("close")
        for _date, smap in reversed(timeline):
            if sym in smap:
                weeks += 1; first_close = smap[sym]
            else:
                break
        age = "Fresh" if weeks <= 1 else f"Week {weeks-1}"
        ret = (s["close"] / first_close - 1) * 100 if (first_close and s.get("close")) else None
        sl_hit = (s.get("close") is not None and s.get("sugg_stop") is not None
                  and s["close"] < s["sugg_stop"])
        out.append({"symbol": sym, "name": s.get("name"), "sector": s.get("sector"),
                    "grade": s.get("grade"), "close": s.get("close"),
                    "weeks": weeks, "age": age, "new": sym not in prev_syms,
                    "ret_pct": round(ret, 1) if ret is not None else None, "sl_hit": sl_hit})
    out.sort(key=lambda x: -x["weeks"])   # longest-held first
    dropped = sorted(sym for sym in prev_syms if sym not in cur)
    return {"as_of": as_of, "stocks": out, "dropped": dropped,
            "first_run": len(timeline) <= 1}

# ------------------------------------------------------------------ RRG (sector rotation quadrant)
def _rolling_z(series, window=26):
    out = []
    for i in range(len(series)):
        w = series[max(0, i-window+1):i+1]
        if len(w) < 8:
            out.append(0.0); continue
        m = mean(w); s = std(w)
        out.append((series[i]-m)/s if s > 0 else 0.0)
    return out

def compute_rrg():
    """Sector rotation vs Nifty 500 — true RS-Ratio / RS-Momentum (de Kempenaer style).
    RS-Ratio: 100 = matching the benchmark; >100 outperforming, <100 lagging.
    RS-Momentum: 100 = RS-Ratio flat; >100 accelerating, <100 decelerating."""
    TRAIL = 12
    bench = fetch("^CRSLDX"); label = "Nifty 500"
    if not bench:
        bench = fetch("^NSEI"); label = "Nifty 50"
    if not bench:
        return {"benchmark": "", "trail_weeks": TRAIL, "sectors": []}
    bmap = {t: c for t, c in bench}
    dates = [t for t, _ in bench]
    out = []
    for s in SECTORS_DIRECT + SECTORS_PROXY:
        ser = fetch(s["ticker"])
        if not ser:
            continue
        m = {t: c for t, c in ser}
        rs = [m[d]/bmap[d] for d in dates if d in m and bmap.get(d)]
        if len(rs) < 40:
            continue
        # RS-Ratio = current relative strength vs its own 14-wk trend, x100.
        # 100 = strength in line with its trend; >100 gaining on Nifty 500, <100 losing.
        N = 14
        rsr = []
        for i in range(len(rs)):
            w = rs[max(0, i-N+1):i+1]
            ma = mean(w)
            rsr.append(100.0 * rs[i] / ma if ma else 100.0)
        # RS-Momentum = RS-Ratio vs its own 10-wk trend (is the strength itself rising?).
        M = 10
        rsm = []
        for i in range(len(rsr)):
            w = rsr[max(0, i-M+1):i+1]
            ma = mean(w)
            rsm.append(100.0 * rsr[i] / ma if ma else 100.0)
        rsr = [round(v, 2) for v in rsr]
        rsm = [round(v, 2) for v in rsm]
        trail = [[rsr[i], rsm[i]] for i in range(len(rsr))][-TRAIL:]
        cx, cy = trail[-1]
        quad = ("Leading" if cx >= 100 and cy >= 100 else
                "Weakening" if cx >= 100 and cy < 100 else
                "Lagging" if cx < 100 and cy < 100 else "Improving")
        out.append({"name": s["name"], "rs_ratio": cx, "rs_mom": cy,
                    "quadrant": quad, "trail": trail, "proxy": s.get("proxy", False)})
    out.sort(key=lambda x: -x["rs_ratio"])
    return {"benchmark": label, "trail_weeks": TRAIL, "sectors": out}

# ------------------------------------------------------------------ TRACK RECORD LEDGER
def compute_ledger(current_stocks, as_of, holdings_roster=None):
    """Forward-tested track record.

    ENTRY  = the close of the week a name FIRST appeared on the Trading buy list.
    OPEN   = still visible in EITHER the Trading scan OR the Holdings scan
             (Holdings = anything still trading above its 150-MA).
    CLOSED = absent from BOTH scans in a week where Holdings data exists
             — i.e. a real 150-MA break, not just a rotation off the buy list.

    Data-gap guard: if a given week has no Holdings roster at all, absence from
    Trading is treated as UNKNOWN and the position is carried forward, never
    closed. Without this, one missing CSV would silently fake a load of exits.
    """
    import glob
    timeline = []   # (date, alive_map, has_holdings)
    for sp in sorted(glob.glob("history/stage2-*.json")):
        try:
            d = json.load(open(sp))
            date = d.get("as_of") or sp.split("stage2-")[-1].replace(".json", "")
            stocks = d.get("scanners", {}).get("trading", {}).get("stocks", []) if "scanners" in d else d.get("stocks", [])
            trading = {s["symbol"]: {"close": s.get("close"), "sma150": s.get("stage2_exit")}
                       for s in (stocks or []) if s.get("symbol")}
            hold = d.get("holdings_roster") or {}
            alive = dict(hold)
            alive.update(trading)          # Trading data wins where both exist
            timeline.append((date, alive, set(trading), bool(hold)))
        except Exception:
            pass
    timeline.sort(key=lambda t: t[0])

    # current week (may not be in the snapshot list yet on a fresh write)
    cur_trading = {s["symbol"]: {"close": s.get("close"), "sma150": s.get("stage2_exit")}
                   for s in (current_stocks or []) if s.get("symbol")}
    cur_hold = dict(holdings_roster or {})
    if not timeline or timeline[-1][0] != as_of:
        alive = dict(cur_hold); alive.update(cur_trading)
        timeline.append((as_of, alive, set(cur_trading), bool(cur_hold)))

    # first appearance on the TRADING list defines an entry
    seen = {}
    for date, alive, trading_syms, has_hold in timeline:
        for sym in trading_syms:
            if sym not in seen:
                seen[sym] = {"entry_date": date, "entry_close": alive[sym]["close"]}

    open_pos, closed = [], []
    for sym, info in seen.items():
        ec = info["entry_close"]
        last_close, last_date, exit_date = ec, info["entry_date"], None
        started = False
        for date, alive, trading_syms, has_hold in timeline:
            if date < info["entry_date"]:
                continue
            started = True
            if sym in alive:
                if alive[sym].get("close") is not None:
                    last_close = alive[sym]["close"]
                last_date = date
            elif has_hold:
                exit_date = date          # off both scans, holdings data present -> real break
                break
            # else: no holdings data this week -> unknown, carry forward
        rec = {"symbol": sym, "entry_date": info["entry_date"], "entry_close": ec,
               "last_close": last_close, "last_date": last_date,
               "ret_pct": round((last_close/ec - 1)*100, 1) if (ec and last_close) else None}
        if exit_date:
            rec["status"] = "stopped"; rec["exit_date"] = exit_date
            closed.append(rec)
        else:
            # still alive; flag whether it is on the buy list or just held above the MA
            rec["status"] = "open"
            rec["on_buylist"] = sym in cur_trading
            open_pos.append(rec)

    rets = [c["ret_pct"] for c in closed if c["ret_pct"] is not None]
    wins = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]
    board = {
        "closed": len(closed), "open": len(open_pos),
        "win_rate": round(len(wins)/len(rets)*100, 0) if rets else None,
        "avg_ret": round(sum(rets)/len(rets), 1) if rets else None,
        "avg_win": round(sum(wins)/len(wins), 1) if wins else None,
        "avg_loss": round(sum(losses)/len(losses), 1) if losses else None,
        "best": max(rets) if rets else None, "worst": min(rets) if rets else None,
    }
    closed.sort(key=lambda x: (x.get("exit_date") or ""), reverse=True)
    open_pos.sort(key=lambda x: -(x["ret_pct"] or 0))
    return {"as_of": as_of, "board": board, "open": open_pos, "closed": closed,
            "holdings_seen": len(cur_hold),
            "first_run": len(timeline) <= 1}

# ------------------------------------------------------------------ BREADTH + WEATHER
def compute_breadth(sample=50):
    """% of a liquid large/mid basket trading above its 30-week (≈150-day) MA."""
    above, total = 0, 0
    for tk in NIFTY200[:sample]:
        ser = fetch(tk)
        if not ser or len(ser) < 32:
            continue
        closes = [c for _, c in ser]
        sma30 = mean(closes[-30:])
        total += 1
        if closes[-1] > sma30:
            above += 1
    return {"pct_above": round(above/total*100) if total else None, "sample": total}

def compute_weather(regime, breadth, trading_grades):
    """GO / SELECTIVE / STAND DOWN. Trend GATES the verdict; breadth, VIX and the
    scan's own Prime/Late health fine-tune GO vs SELECTIVE."""
    up = regime.get("trend_up")
    vix = regime.get("vix"); band = regime.get("vix_band")
    pa = (breadth or {}).get("pct_above")
    g = trading_grades or {}
    prime, late = g.get("Prime", 0), g.get("Late", 0)
    pl = prime/late if late else (prime if prime else 0)

    fine = []
    if pa is not None:  fine.append(1 if pa >= 60 else -1 if pa <= 35 else 0)
    if vix is not None: fine.append(1 if vix < 14 else -1 if vix > 20 else 0)
    fine.append(1 if pl >= 1 else -1 if pl < 0.4 else 0)
    score = sum(fine)/len(fine) if fine else 0

    if not up:
        verdict, cls = "STAND DOWN", "off"
        action = ("Nifty is below its 30-week trend line. Preserve capital — momentum setups "
                  "whipsaw in a downtrend. Sit out until the trend turns back up.")
    elif score >= 0.4:
        verdict, cls = "GO", "on"
        action = "Trend up and conditions broad. Press your best fresh-reclaim Prime setups at full size."
    elif score <= -0.3:
        verdict, cls = "SELECTIVE", "neutral"
        action = "Trend up but internals are mixed — thin breadth, high fear, or a tired list. Freshest Prime names only, half size."
    else:
        verdict, cls = "SELECTIVE", "neutral"
        action = "Trend up, conditions okay but not exceptional. Be choosy — top setups only, normal size."

    return {"verdict": verdict, "cls": cls, "action": action, "score": round(score, 2),
            "trend_up": up, "pct_vs_ma": regime.get("pct_vs_ma"),
            "pct_above": pa, "vix": vix, "vix_band": band,
            "prime": prime, "late": late}

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
    print("Building sector RRG…")
    rrg = compute_rrg()
    print(f"  RRG: {len(rrg['sectors'])} sectors vs {rrg['benchmark']}")
    print("Measuring market breadth…")
    breadth = compute_breadth()
    print(f"  breadth: {breadth['pct_above']}% above 30-wk MA ({breadth['sample']} names)")
    payload = {
        "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "as_of": as_of,
        "regime": regime,
        "sectors": results,
        "rrg": rrg,
        "breadth": breadth,
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

    # ---- Page 1: stocks — route each CSV to its scanner by filename ----
    import glob
    csv_files = sorted(glob.glob("*.csv"))
    scanners = {}
    for sc in SCANNERS:
        match = None
        for f in csv_files:
            fl = f.lower()
            if any(k in fl for k in sc["keywords"]):
                match = f; break
        entry = {"label": sc["label"], "style": sc["style"],
                 "personal": sc["personal"], "note": sc["note"], "qualified": 0, "stocks": []}
        if match:
            try:
                parsed = parse_chartink_csv(match)
                entry.update(parsed)
                print(f"  {sc['id']:<13} <- {match}  ({parsed['qualified']} names)")
            except Exception as e:
                print(f"  {sc['id']:<13} skip {match}: {e}")
        else:
            print(f"  {sc['id']:<13} <- (no CSV — empty state)")
        scanners[sc["id"]] = entry

    # ---- Holdings scan (silent plumbing): everything still above its 150-MA ----
    holdings_roster = {}
    hmatch = next((f for f in csv_files
                   if any(k in f.lower() for k in HOLDINGS_KEYWORDS)), None)
    if hmatch:
        try:
            holdings_roster = parse_holdings_csv(hmatch)
            print(f"  {'holdings':<13} <- {hmatch}  ({len(holdings_roster)} names above 150-MA)")
        except Exception as e:
            print(f"  {'holdings':<13} skip {hmatch}: {e}")
    else:
        print(f"  {'holdings':<13} <- (no CSV — ledger carries open picks forward, will NOT close them)")

    stage2 = {"scanners": scanners, "generated_at": payload["generated_at"], "as_of": as_of,
              "holdings_roster": holdings_roster}
    # merge persisted Top-5 fundamental notes (written via /hariskill), if present
    if os.path.exists("fundamentals.json"):
        try:
            funds = json.load(open("fundamentals.json"))
            for scv in scanners.values():
                for st in scv.get("stocks", []):
                    if st["symbol"] in funds:
                        st["fundamental"] = funds[st["symbol"]]
            print(f"  merged {len(funds)} fundamental notes")
        except Exception as e:
            print(f"  fundamentals merge skipped: {e}")
    os.makedirs("history", exist_ok=True)
    # snapshot first so this week is in the aging timeline
    with open(f"history/stage2-{as_of}.json", "w") as f:
        json.dump(stage2, f, indent=2)
    # tracker reads all snapshots (including this one)
    stage2["tracker"] = compute_tracker(scanners.get("trading", {}).get("stocks", []), as_of)
    stage2["ledger"] = compute_ledger(scanners.get("trading", {}).get("stocks", []), as_of,
                                      holdings_roster)
    stage2["weather"] = compute_weather(regime, breadth, scanners.get("trading", {}).get("grades", {}))
    print(f"  weather: {stage2['weather']['verdict']} (score {stage2['weather']['score']}, "
          f"breadth {stage2['weather']['pct_above']}%, trend_up {stage2['weather']['trend_up']})")
    with open("stage2.json", "w") as f:
        json.dump(stage2, f, indent=2)
    print(f"  tracker: {len(stage2['tracker']['stocks'])} tracked, "
          f"{len(stage2['tracker']['dropped'])} dropped, first_run={stage2['tracker']['first_run']}")
    b = stage2["ledger"]["board"]
    print(f"  ledger: {b['open']} open, {b['closed']} closed, win_rate={b['win_rate']}, "
          f"holdings_roster={len(holdings_roster)}")

    print(f"\nDone. as_of={as_of}  sectors_ranked={n}  "
          f"loaded={payload['diagnostics']['loaded']}/{payload['diagnostics']['total']}")

if __name__ == "__main__":
    main()
