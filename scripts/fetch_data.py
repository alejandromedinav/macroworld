#!/usr/bin/env python3
"""
Macro Soundings - daily data build.

Pulls the day's figures from FRED, merges them with the hand-written fields
in manual.json, and injects the result into index.html between the
DATA:START / DATA:END markers.

Standard library only - nothing to install.

    FRED_API_KEY=xxx python3 scripts/fetch_data.py

If FRED is unreachable this exits non-zero and writes nothing, so the
previously deployed page stays up with the last good numbers.
"""

import datetime
import json
import os
import ssl
import sys
import urllib.parse
import urllib.request
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_dotenv():
    """Read KEY=VALUE lines from .env if it exists. A real environment
    variable always wins, so CI is unaffected."""
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


load_dotenv()
KEY = os.environ.get("FRED_API_KEY")
EIA_KEY = os.environ.get("EIA_API_KEY")   # optional; see fetch_eia below
AV_KEY  = os.environ.get("ALPHAVANTAGE_API_KEY")   # optional; see fetch_alphavantage
_AV_LAST = 0.0                                    # throttle marker for the 5/min limit

# macOS Python from python.org ships without a CA bundle, so TLS fails with
# CERTIFICATE_VERIFY_FAILED even though curl works. Prefer certifi's bundle
# when it is installed; otherwise fall back to the system default.
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    if SSL_CTX.cert_store_stats()["x509_ca"] == 0:
        sys.exit(
            "No trusted CA certificates available to Python.\n"
            "  macOS fix:  run '/Applications/Python 3.x/Install Certificates.command'\n"
            "  or:         python3 -m pip install certifi"
        )

if not KEY:
    sys.exit(
        "FRED_API_KEY is not set.\n"
        "  Local:  create a .env file next to index.html containing\n"
        "            FRED_API_KEY=your_key\n"
        "          (.env is gitignored, so it never leaves your machine)\n"
        "  CI:     repo Settings > Secrets and variables > Actions > New repository secret"
    )

# ---------------------------------------------------------------------------
# CPI relative importances and PCE shares.
# These are NOT available as FRED series - they come from the BLS
# relative-importance tables and BEA underlying detail, and they change once
# a year. Update them each January. Each column must sum to 100.
# ---------------------------------------------------------------------------
WEIGHTS = {
    "shelter":   {"cpi": 34.9, "pce": 15.4},
    "food":      {"cpi": 13.6, "pce": 7.6},
    "medical":   {"cpi": 8.1,  "pce": 17.2},
    "transport": {"cpi": 16.4, "pce": 9.0},
    "energy":    {"cpi": 6.6,  "pce": 3.8},
    "other":     {"cpi": 20.4, "pce": 47.0},
}

SERIES = {
    "y3m":        ("DGS3MO", None),
    "y2y":        ("DGS2", None),
    "y10y":       ("DGS10", None),
    "y30y":       ("DGS30", None),
    "funds":      ("DFF", None),                    # effective rate actually traded
    "tgt_lo":     ("DFEDTARL", None),               # published FOMC target range, not derived
    "tgt_hi":     ("DFEDTARU", None),
    "hy_oas":     ("BAMLH0A0HYM2", None),
    "hy_yield":   ("BAMLH0A0HYM2EY", None),         # effective yield: OAS is over the whole curve,
                                                    # so 10Y + OAS is not the borrowing cost
    "wti":        ("DCOILWTICO", None),
    "brent":      ("DCOILBRENTEU", None),
    "cpi":        ("CPIAUCSL", "pc1"),
    "pce":        ("PCEPI", "pc1"),
    "breakeven":  ("T10YIE", None),
    "real_tips":  ("DFII10", None),
    "debt_gdp":   ("GFDEGDQ188S", None),
    "debt_level": ("GFDEBTN", None),               # the balance itself, in millions

    "airfare":    ("CUSR0000SETG01", "pc1"),
    "c_shelter":  ("CUSR0000SAH1", "pc1"),
    "c_food":     ("CPIUFDSL", "pc1"),
    "c_medical":  ("CPIMEDSL", "pc1"),
    "c_transport":("CPITRNSL", "pc1"),
    "c_energy":   ("CPIENGSL", "pc1"),
}


class Obs:
    """The most recent usable observations for one series, newest first."""

    def __init__(self, values, dates):
        self.values = values
        self.dates = dates

    @property
    def value(self):
        return self.values[0]

    @property
    def date(self):
        return self.dates[0]

    def at(self, n):
        """Value n prints back, or the latest if the history is short."""
        return self.values[n] if n < len(self.values) else self.values[0]

    def change(self, n=1):
        """Move over n prints, or None when there is no prior value to compare."""
        return None if len(self.values) <= n else self.values[0] - self.values[n]


def fetch(series_id, units=None):
    params = {
        "series_id": series_id,
        "api_key": KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": "12",
    }
    if units:
        params["units"] = units
    url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={"User-Agent": "macro-soundings"})
    with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
        body = json.load(resp)

    # FRED writes "." for holidays and gaps - drop them before reading.
    good = [o for o in body.get("observations", []) if o["value"] != "."]
    if not good:
        raise RuntimeError(f"{series_id}: no usable observations returned")
    return Obs([float(o["value"]) for o in good], [o["date"] for o in good])


def fetch_eia(series):
    """Spot prices straight from the EIA.

    FRED's DCOILWTICO/DCOILBRENTEU are EIA data re-published, so this exists to
    answer one question with evidence rather than argument: does EIA post ahead
    of FRED's ingest? If it does, the fresher print wins. If it doesn't, nothing
    changes and the build carries on with FRED.

    Never fatal - any failure falls back to FRED.
    """
    if not EIA_KEY:
        return None
    params = [
        ("api_key", EIA_KEY), ("frequency", "daily"), ("data[0]", "value"),
        ("facets[series][]", series),
        ("sort[0][column]", "period"), ("sort[0][direction]", "desc"),
        ("length", "12"),
    ]
    url = "https://api.eia.gov/v2/petroleum/pri/spt/data/?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "macro-soundings"})
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            rows = json.load(resp)["response"]["data"]
        rows = [r for r in rows if r.get("value") is not None]
        if not rows:
            return None
        return Obs([float(r["value"]) for r in rows], [r["period"] for r in rows])
    except Exception as exc:
        print(f"  EIA lookup for {series} failed ({exc}); using FRED instead")
        return None


def fetch_alphavantage(function):
    """Alpha Vantage's WTI / BRENT series.

    Worth knowing before trusting it: several free "oil price" APIs resell the
    same EIA spot series, so this is checked against FRED by date like every
    other candidate rather than assumed to be fresher. Never fatal.
    """
    if not AV_KEY:
        return None
    # Free tier allows 5 requests/minute and rejects bursts. Two calls back to
    # back is enough to trip it, so space them.
    global _AV_LAST
    wait = 1.5 - (time.monotonic() - _AV_LAST)
    if wait > 0:
        time.sleep(wait)
    _AV_LAST = time.monotonic()
    params = {"function": function, "interval": "daily", "apikey": AV_KEY}
    url = "https://www.alphavantage.co/query?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "macro-soundings"})
        with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as resp:
            body = json.load(resp)
        # Rate limits and errors come back as prose in Note/Information, not as HTTP errors.
        for key in ("Note", "Information", "Error Message"):
            if key in body:
                print(f"  Alpha Vantage {function}: {str(body[key])[:120]}")
                return None
        rows = [r for r in body.get("data", []) if r.get("value") not in (None, ".", "")]
        if not rows:
            return None
        return Obs([float(r["value"]) for r in rows], [r["date"] for r in rows])
    except Exception as exc:
        print(f"  Alpha Vantage {function} failed ({exc}); falling back")
        return None


def fetch_all():
    def one(item):
        name, (sid, units) = item
        try:
            return name, fetch(sid, units)
        except Exception as exc:
            raise RuntimeError(f"Failed on {sid} ({name}): {exc}") from exc

    with ThreadPoolExecutor(max_workers=8) as pool:
        return dict(pool.map(one, SERIES.items()))


def r(n, d=2):
    return round(n, d)


def archive_manual_oil(manual):
    """Roll today's hand-entered prices into a short history.

    You only ever edit asOf/wti/brent. This keeps the previous entries so the
    build can compute a real day-on-day change instead of showing nothing, and
    caps the list so manual.json does not grow without bound.
    """
    o = manual.get("oil") or {}
    when, wti, brent = (o.get("asOf") or "").strip(), o.get("wti"), o.get("brent")
    if not when or wti is None or brent is None:
        return manual
    hist = [h for h in (o.get("history") or []) if h.get("asOf") != when]
    hist.append({"asOf": when, "wti": wti, "brent": brent})
    hist.sort(key=lambda h: h.get("asOf", ""), reverse=True)
    o["history"] = hist[:30]
    manual["oil"] = o
    return manual


def prior_manual(manual, which, when):
    """The most recent hand-entered price from a date before `when`.

    Picks by maximum date rather than list position, so a history that has been
    hand-edited or reordered by a merge still compares against the right day.
    """
    best = None
    for h in (manual.get("oil") or {}).get("history") or []:
        stamp = h.get("asOf", "")
        if stamp < when and h.get(which) is not None:
            if best is None or stamp > best[1]:
                best = (float(h[which]), stamp)
    return best if best else (None, None)


def apply_curve_override(curve, manual, dates):
    """Let a yield be entered by hand when it has moved ahead of FRED's publication.

    Same rule as oil: only used while strictly newer than FRED's print, and the
    per-key vintage is restamped so a mixed-date curve is visible on the page
    rather than hidden. Optional note and alert ride along for display.
    """
    o = manual.get("curve") or {}
    when = (o.get("asOf") or "").strip()
    vals = o.get("values") or {}
    if not when or not vals:
        return curve
    notes, alerts = (o.get("notes") or {}), set(o.get("alert") or [])
    key_to_date = {"3M": "y3m", "2Y": "y2y", "10Y": "y10y", "30Y": "y30y"}
    for point in curve:
        k = point["key"]
        if k not in vals or vals[k] is None:
            continue
        fred_date = dates.get(key_to_date[k], "")
        if when <= fred_date:
            print(f"  {k}: hand entry {when} is not ahead of FRED {fred_date} - using FRED")
            continue
        print(f"  {k}: using your hand entry {vals[k]}% ({when})")
        point["yield"] = r(float(vals[k]))
        point.pop("chg", None)          # no prior hand print to compare against
        dates[key_to_date[k]] = when
        if notes.get(k):
            point["note"] = notes[k]
        if k in alerts:
            point["alert"] = True
    return curve


def manual_oil(manual, which):
    """A price you read and typed in yourself.

    A person reading a published number is not automated collection, so this is
    the one route to same-day oil with no terms attached. It is used only while
    it is newer than FRED - stop updating and the build quietly reverts to FRED
    rather than presenting a stale hand-entry as current.
    """
    o = manual.get("oil") or {}
    val, when = o.get(which), (o.get("asOf") or "").strip()
    if val is None or not when:
        return None
    try:
        vals, dates = [float(val)], [when]
        prev, prev_when = prior_manual(manual, which, when)
        if prev is not None:
            vals.append(prev)
            dates.append(prev_when)
        return Obs(vals, dates)
    except (TypeError, ValueError):
        print(f"  manual oil {which}: '{val}' is not a number; ignoring")
        return None


def freshest(name, fred_obs, candidates):
    """Pick the most recent print among FRED and any optional sources.

    candidates: list of (label, callable) tried in order. A source is only used
    if its latest observation is genuinely newer than FRED's - otherwise this
    quietly stays on FRED, so adding a key can never make the data worse.
    """
    best, best_label = fred_obs, "EIA via FRED"
    for label, fetch in candidates:
        got = fetch()
        if not got:
            continue
        if got.date > best.date:
            print(f"  {name}: {label} is ahead ({got.date} vs {best.date}) - using it")
            best, best_label = got, label
        else:
            print(f"  {name}: {label} is not ahead ({got.date} vs {best.date})")
    return best, best_label


def _oil_entry(o):
    """Price, plus the move and percent move when there is a prior print."""
    out = {"price": r(o.value)}
    chg = o.change()
    if chg is not None:
        out["chg"] = r(chg)
        prev = o.at(1)
        if prev:
            out["pct"] = r(chg / prev * 100, 2)   # standard: change over the OLD price
    return out


def build(S, manual):
    # --- components: measured lines, then "other" as the reconciling residual ---
    measured = {
        "shelter":   r(S["c_shelter"].value, 1),
        "food":      r(S["c_food"].value, 1),
        "medical":   r(S["c_medical"].value, 1),
        "transport": r(S["c_transport"].value, 1),
        "energy":    r(S["c_energy"].value, 1),
    }
    headline_cpi = S["cpi"].value
    headline_pce = S["pce"].value

    # "Other goods and services" is not a single FRED series, so it is solved as
    # the residual that makes the basket reconcile to the published headline.
    # It has to be solved SEPARATELY for each measure: the same residual applied
    # at PCE weights would not tie out, because PCE's 47% "other" bucket covers
    # very different ground from CPI's 20%. Each basket ties to its own headline.
    known_cpi = sum(WEIGHTS[k]["cpi"] * v for k, v in measured.items())
    known_pce = sum(WEIGHTS[k]["pce"] * v for k, v in measured.items())
    other_cpi = (headline_cpi * 100 - known_cpi) / WEIGHTS["other"]["cpi"]
    other_pce = (headline_pce * 100 - known_pce) / WEIGHTS["other"]["pce"]

    yoy = dict(measured, other=r(other_cpi, 1))

    shapes = [
        ("shelter", "Shelter", "houses"),
        ("food", "Food", "market"),
        ("medical", "Medical care", "hospital"),
        ("transport", "Transportation", "transit"),
        ("energy", "Energy", "fuel"),
        ("other", "Other goods & services", "shops"),
    ]
    components = []
    for k, name, kind in shapes:
        c = {"key": k, "name": name, "cpi": WEIGHTS[k]["cpi"], "pce": WEIGHTS[k]["pce"],
             "yoy": yoy[k], "kind": kind}
        if k == "other":
            c["yoyPce"] = r(other_pce, 1)   # the residual differs by measure
        components.append(c)

    def pick_oil(name, which, fred_obs):
        """Front-month futures are the chosen series, so a hand entry always wins.

        Falling back to EIA spot when an entry goes stale would silently swap the
        instrument - spot and futures are different prices - and would make the
        day-on-day change compare two unlike things. Better to show a dated
        futures print, stamped with its real date, and complain loudly.
        """
        m = manual_oil(manual, which)
        if m:
            age = (datetime.date.today() - datetime.date.fromisoformat(m.date)).days
            if age > 4:
                print(f"  {name}: hand entry is {age} days old ({m.date}) - update manual.json")
            else:
                print(f"  {name}: using your futures entry ({m.date})")
            return m, "Manual (front-month futures)"
        return freshest(name, fred_obs, [
            ("Alpha Vantage futures", lambda: fetch_alphavantage("WTI" if which == "wti" else "BRENT")),
            ("EIA (direct)",          lambda: fetch_eia("RWTC" if which == "wti" else "RBRTE")),
        ])

    wti_obs, wti_src = pick_oil("WTI", "wti", S["wti"])
    brent_obs, brent_src = pick_oil("Brent", "brent", S["brent"])
    S["wti"], S["brent"] = wti_obs, brent_obs

    # Frequencies differ wildly - Treasuries daily, CPI monthly, debt quarterly -
    # so a single "as at" date would misrepresent most of the page.
    dates = {k: S[k].date for k in S}

    curve = [
        {"key": "3M",  "label": "3-month bill", "yield": r(S["y3m"].value),
         "chg": r(S["y3m"].value - S["y3m"].at(1))},
        {"key": "2Y",  "label": "2-year note",  "yield": r(S["y2y"].value),
         "chg": r(S["y2y"].value - S["y2y"].at(1))},
        {"key": "10Y", "label": "10-year note", "yield": r(S["y10y"].value),
         "chg": r(S["y10y"].value - S["y10y"].at(1))},
        {"key": "30Y", "label": "30-year bond", "yield": r(S["y30y"].value),
         "chg": r(S["y30y"].value - S["y30y"].at(1))},
    ]
    curve = apply_curve_override(curve, manual, dates)

    return {
        "asOf": S["y10y"].date,
        # FRED publishes Treasury data a day or two behind, so the data's own
        # date and the day we fetched it are different facts. Show both.
        "builtAt": datetime.date.today().isoformat(),
        "summary": manual["summary"],
        "author": manual.get("author", ""),
        "maker": manual.get("credit", ""),   # note: DATA["credit"] is the HY block
        "curve": curve,
        "oil": {
            "wti":   _oil_entry(S["wti"]),
            "brent": _oil_entry(S["brent"]),
        },
        "dates": dates,
        "oilSource": wti_src,
        "fed": {
            "lower": r(S["tgt_lo"].value),
            "upper": r(S["tgt_hi"].value),
            "effective": r(S["funds"].value),
            "lastMove": manual["fed"]["lastMove"],
            "lastDate": manual["fed"]["lastDate"],
            "nextDate": manual["fed"]["nextDate"],
            "stance": manual["fed"].get("stance", "neutral"),
        },
        "credit": {
            "effYield": r(S["hy_yield"].value),
            "oas": round(S["hy_oas"].value * 100),
            "chg": round((S["hy_oas"].value - S["hy_oas"].at(5)) * 100),
        },
        "breakeven": r(S["breakeven"].value),
        "realTips": r(S["real_tips"].value),
        "debt": {
            "gdp": r(S["debt_gdp"].value, 1),
            # GFDEGDQ188S is quarterly, so this is the change over four prints.
            "chg": r(S["debt_gdp"].value - S["debt_gdp"].at(4), 1),
            # The ratio is the shape on the map; the balance is what people quote.
            # GFDEBTN is in millions and shares the ratio's quarterly vintage.
            "level": r(S["debt_level"].value / 1e6, 2),
        },
        "airfare": {
            "yoy": r(S["airfare"].value, 1),
            "chg": r(S["airfare"].value - S["airfare"].at(1), 1),
        },
        # chg is the move in the year-over-year rate itself: did inflation accelerate?
        "inflation": {
            "cpi": r(headline_cpi, 1), "cpiChg": r(headline_cpi - S["cpi"].at(1), 1),
            "pce": r(S["pce"].value, 1), "pceChg": r(S["pce"].value - S["pce"].at(1), 1),
        },
        "ai": manual["ai"],   # hand-entered; no FRED equivalent
        "watch": manual["watch"],
        "components": components,
    }


HEADER = (
    "/* === DATA:START - generated by scripts/fetch_data.py. Do not edit by hand; ===\n"
    "   === edit manual.json for the read, the watch list and AI capex.        === */\n"
)
END = "/* === DATA:END === */"


def inject(data):
    """Rewrites only the DATA block between the markers.

    META GUARD: everything above the markers - title, description, Open Graph
    tags - is untouched by the build, so link previews survive a refresh.
    """
    path = ROOT / "index.html"
    html = path.read_text(encoding="utf8")
    a = html.find("/* === DATA:START")
    b = html.find(END)
    if a == -1 or b == -1:
        raise RuntimeError("DATA markers not found in index.html")
    block = HEADER + "var DATA = " + json.dumps(data, indent=2, ensure_ascii=False) + ";\n"
    path.write_text(html[:a] + block + html[b:], encoding="utf8")


DEBT_PENNY = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
              "/v2/accounting/od/debt_to_penny")


def fetch_debt_to_penny(start):
    """Total public debt outstanding, daily, straight from Treasury.

    FRED's GFDEBTN is the same balance sampled quarterly and published with a
    long lag, so it reads months stale as a headline. This is the number people
    actually quote, and it needs no key.
    """
    q = urllib.parse.urlencode({
        "sort": "record_date",
        "page[size]": "10000",
        "fields": "record_date,tot_pub_debt_out_amt",
        "filter": "record_date:gte:" + start,
    })
    req = urllib.request.Request(DEBT_PENNY + "?" + q, headers={"User-Agent": "macro-soundings"})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as resp:
        body = json.load(resp)
    return [(r["record_date"], float(r["tot_pub_debt_out_amt"]) / 1e12)
            for r in body.get("data", []) if r.get("tot_pub_debt_out_amt")]


# ---------------------------------------------------------------- history ----
# The map is a snapshot; this is what lets you drag it back through time. Two
# years sampled weekly is smooth enough to scrub and small enough to inline:
# roughly 2,300 numbers, a few KB once gzipped. Daily would be seven times that
# for no visible gain at this scale.

HISTORY_WEEKS = 104

HISTORY_KEYS = [
    "y3m", "y2y", "y10y", "y30y", "funds", "tgt_lo", "tgt_hi",
    "hy_oas", "hy_yield", "wti", "brent", "cpi", "pce",
    "breakeven", "real_tips", "debt_gdp", "debt_level", "airfare",
    "c_shelter", "c_food", "c_medical", "c_transport", "c_energy",
]


def fetch_series_history(series_id, units, start):
    """Every observation since `start`, oldest first."""
    params = {
        "series_id": series_id,
        "api_key": KEY,
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": start,
        "limit": "3000",
    }
    if units:
        params["units"] = units
    url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "macro-soundings"})
    with urllib.request.urlopen(req, timeout=45, context=SSL_CTX) as resp:
        body = json.load(resp)
    return [(o["date"], float(o["value"]))
            for o in body.get("observations", []) if o["value"] != "."]


def weekly_axis(end_iso, weeks):
    end = datetime.date.fromisoformat(end_iso)
    return [(end - datetime.timedelta(days=7 * i)).isoformat()
            for i in range(weeks - 1, -1, -1)]


def sample_onto(pairs, axis):
    """Last known value on or before each axis date, carried forward.

    Monthly and quarterly series step rather than glide, which is honest: the
    number really did not change between prints.
    """
    if not pairs:
        return None
    out, i, cur = [], 0, None
    for d in axis:
        while i < len(pairs) and pairs[i][0] <= d:
            cur = pairs[i][1]
            i += 1
        out.append(cur)
    first = next((v for v in out if v is not None), None)
    if first is None:
        return None
    return [round(v if v is not None else first, 2) for v in out]


def build_history(as_of):
    """Weekly history for the scrubber.

    A failure here degrades the feature rather than the build: the site is
    perfectly usable without the time slider, so one flaky series should not
    cost the day's refresh.
    """
    axis = weekly_axis(as_of, HISTORY_WEEKS)
    start = (datetime.date.fromisoformat(axis[0]) - datetime.timedelta(days=400)).isoformat()
    series, missing = {}, []
    for key in HISTORY_KEYS:
        series_id, units = SERIES[key]
        got = None
        for attempt in (1, 2):
            try:
                got = sample_onto(fetch_series_history(series_id, units, start), axis)
                break
            except Exception as exc:
                if attempt == 2:
                    print(f"  history: {key} ({series_id}) unavailable - {exc}")
                else:
                    time.sleep(1.5)
        if got:
            series[key] = got
        else:
            missing.append(key)
    latest_debt = None
    try:
        penny = fetch_debt_to_penny(start)
        sampled = sample_onto(penny, axis)
        if sampled:
            series["debt_penny"] = sampled
            latest_debt = round(penny[-1][1], 2)
            print(f"  debt outstanding ${latest_debt}tn (Treasury, {penny[-1][0]})")
    except Exception as exc:
        print(f"  debt to the penny unavailable, falling back to FRED's quarterly - {exc}")
    if missing:
        print(f"  history: skipping {', '.join(missing)}; the slider will hold these flat")
    return {"weeks": axis, "series": series, "latestDebt": latest_debt}


def main():
    S = fetch_all()
    manual_path = ROOT / "manual.json"
    manual = json.loads(manual_path.read_text(encoding="utf8"))
    before = json.dumps(manual, sort_keys=True)
    manual = archive_manual_oil(manual)
    if json.dumps(manual, sort_keys=True) != before:
        manual_path.write_text(json.dumps(manual, indent=2, ensure_ascii=False) + "\n", encoding="utf8")
    data = build(S, manual)
    data["history"] = build_history(data["asOf"])
    # Prefer Treasury's daily balance over FRED's quarterly one for the headline.
    if data["history"].get("latestDebt"):
        data["debt"]["level"] = data["history"]["latestDebt"]
        data["debt"]["levelSource"] = "Treasury, daily"
    inject(data)

    slope = data["curve"][2]["yield"] - data["curve"][1]["yield"]
    spread = data["oil"]["brent"]["price"] - data["oil"]["wti"]["price"]
    real_policy = data["fed"]["effective"] - data["inflation"]["cpi"]
    print(f"Built for {data['asOf']}")
    print(f"  10Y {data['curve'][2]['yield']}%   2s10s {r(slope)}")
    print(f"  CPI {data['inflation']['cpi']}%   PCE {data['inflation']['pce']}%")
    print(f"  HY {data['credit']['oas']}bp   Brent-WTI ${r(spread)}")
    print(f"  Debt/GDP {data['debt']['gdp']}% (as at {data['dates']['debt_gdp']})")
    print(f"  Funds {data['fed']['lower']}-{data['fed']['upper']} (eff {data['fed']['effective']})   ex-post real policy {r(real_policy)}")
    print(f"  HY effective yield {data['credit']['effYield']}%   OAS {data['credit']['oas']}bp")
    print(f"  oil source: {data['oilSource']} (as at {data['dates']['wti']})")
    if "Manual" not in data["oilSource"]:
        print("  NOTE: no hand entry found - falling back to EIA spot, which is a different instrument.")
    other = data["components"][5]
    print(f"  residual 'other'  CPI {other['yoy']}%   PCE {other['yoyPce']}%  (each ties to its own headline)")
    if abs(other["yoy"]) > 6 or abs(other["yoyPce"]) > 6:
        print("  NOTE: a large residual usually means the WEIGHTS constants are stale.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Build failed, nothing written: {exc}")
