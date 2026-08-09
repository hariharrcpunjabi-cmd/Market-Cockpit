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
    {"name": "Indian Stocks vs Gold", "kind": "ratio", "a": "^NSEI", "b": "GOLDBEES.NS",
     "dir": "on", "w": 1.0, "show": "change",
     "why": "When stocks outrun gold, risk appetite is healthy.",
     "up": "Indian stocks are beating gold — money prefers equities over safe-havens. Good sign.",
     "down": "Gold is beating Indian stocks — money is hiding in safe-havens. Defensive sign.",
     "flat": "Stocks and gold roughly even — no clear preference."},
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

        # US 10Y scale fix: Yahoo ^TNX sometimes quotes yield ×10 (e.g. 46.8 = 4.68%)
        if m["show"] == "yield" and val > 20:
            val = val / 10

        # headline number the user can verify against reality
        if m["show"] == "ratio":
            headline = f"{val:.1f}"
        elif m["show"] == "yield":
            headline = f"{val:.2f}%"
        else:  # change
            headline = f"{chg:+.1f}%"

        # risk-on contribution and green/amber/red status
        contrib = z if m["dir"] == "on" else -z
        status = "good" if contrib > 0.7 else "bad" if contrib < -0.7 else "mid"
        plain = m["up"] if arrow == "up" else m["down"] if arrow == "down" else m["flat"]

        signals.append({
            "name": m["name"], "headline": headline, "arrow": arrow, "status": status,
            "plain": plain, "why": m["why"], "chg13": round(chg, 1), "z": round(z, 2),
        })
        score_num += m["w"] * contrib; score_den += m["w"]

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
    return "Prime" if ext <= 10 else "Healthy" if ext <= 20 else "Late"

def _sec_norm(s):
    if not s: return ""
    s = s.strip().title()
    return {"Fmcg": "FMCG", "It": "IT", "Nbfc": "NBFC"}.get(s, s)

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
            # grade-dependent stop (Hariskill rule)
            if g == "Prime":
                stop = sma150 * 0.97; stop_type = "150-MA line"
            else:
                stop = (close - 2*atr) if atr else close * 0.92; stop_type = "2×ATR"
            gc[g] += 1; tc[t] += 1
            if sec: sc[sec] = sc.get(sec, 0) + 1
            rows.append({
                "symbol": sym, "name": (r.get('Stock Name') or '').strip(),
                "close": round(close, 2), "pct_change": _cnum(r.get('%_change')),
                "grade": g, "ext_above_30wma": round(ext, 1),
                "mcap_cr": round(mcap) if mcap else None, "tier": t, "sector": sec,
                "stop": round(stop, 2), "stop_type": stop_type,
                "risk_pct": round((close-stop)/close*100, 1),
                "stage2_exit": round(sma150, 2),
            })
    grank = {"Prime":0,"Healthy":1,"Late":2}
    rows.sort(key=lambda x: (grank[x["grade"]], x["ext_above_30wma"]))
    return {"universe": "Chartink · CLAUDE PROJECT_TRADING", "source": "chartink",
            "scanned": len(rows), "failed": 0, "qualified": len(rows),
            "grades": gc, "tiers": tc, "sector_mix": sc, "stocks": rows}

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

    # ---- Page 1: stocks — any Chartink CSV in the repo, else native Yahoo scan ----
    import glob
    stage2 = None
    for csv_path in sorted(glob.glob("*.csv")):
        try:
            r = parse_chartink_csv(csv_path)
            if r["qualified"] > 0:
                print(f"Using Chartink CSV: {csv_path}")
                stage2 = r; break
        except Exception as e:
            print(f"  skip {csv_path}: {e}")
    if stage2 is None:
        print("No usable Chartink CSV — running native Yahoo Stage 2 scan.")
        stage2 = scan_stage2()
    stage2["generated_at"] = payload["generated_at"]
    stage2["as_of"] = as_of
    with open("stage2.json", "w") as f:
        json.dump(stage2, f, indent=2)
    with open(f"history/stage2-{as_of}.json", "w") as f:
        json.dump(stage2, f, indent=2)

    print(f"\nDone. as_of={as_of}  sectors_ranked={n}  "
          f"loaded={payload['diagnostics']['loaded']}/{payload['diagnostics']['total']}")

if __name__ == "__main__":
    main()
