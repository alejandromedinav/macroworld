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

import json
import os
import ssl
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEY = os.environ.get("FRED_API_KEY")

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
        "  Local:  export FRED_API_KEY=your_key    (or put it in .env, which is gitignored)\n"
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
    "funds":      ("DFF", None),
    "hy_oas":     ("BAMLH0A0HYM2", None),
    "wti":        ("DCOILWTICO", None),
    "brent":      ("DCOILBRENTEU", None),
    "cpi":        ("CPIAUCSL", "pc1"),
    "pce":        ("PCEPI", "pc1"),
    "breakeven":  ("T10YIE", None),
    "real_tips":  ("DFII10", None),
    "debt_gdp":   ("GFDEGDQ188S", None),
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

    # DFF is the effective rate; the target range brackets it in 25bp steps.
    lower = r((S["funds"].value // 0.25) * 0.25)

    return {
        "asOf": S["y10y"].date,
        "summary": manual["summary"],
        "curve": [
            {"key": "3M",  "label": "3-month bill", "yield": r(S["y3m"].value)},
            {"key": "2Y",  "label": "2-year note",  "yield": r(S["y2y"].value)},
            {"key": "10Y", "label": "10-year note", "yield": r(S["y10y"].value),
             "chg": r(S["y10y"].value - S["y10y"].at(1))},
            {"key": "30Y", "label": "30-year bond", "yield": r(S["y30y"].value)},
        ],
        "oil": {
            "wti":   {"price": r(S["wti"].value),   "chg": r(S["wti"].value - S["wti"].at(1))},
            "brent": {"price": r(S["brent"].value), "chg": r(S["brent"].value - S["brent"].at(1))},
        },
        "fed": {
            "lower": lower,
            "upper": r(lower + 0.25),
            "lastMove": manual["fed"]["lastMove"],
            "lastDate": manual["fed"]["lastDate"],
            "nextDate": manual["fed"]["nextDate"],
        },
        "credit": {
            "oas": round(S["hy_oas"].value * 100),
            "chg": round((S["hy_oas"].value - S["hy_oas"].at(5)) * 100),
        },
        "breakeven": r(S["breakeven"].value),
        "realTips": r(S["real_tips"].value),
        "debt": {
            "gdp": r(S["debt_gdp"].value, 1),
            # GFDEGDQ188S is quarterly, so this is the change over four prints.
            "chg": r(S["debt_gdp"].value - S["debt_gdp"].at(4), 1),
        },
        "airfare": {
            "yoy": r(S["airfare"].value, 1),
            "chg": r(S["airfare"].value - S["airfare"].at(1), 1),
        },
        "inflation": {"cpi": r(headline_cpi, 1), "pce": r(S["pce"].value, 1)},
        "ai": manual["ai"],
        "watch": manual["watch"],
        "components": components,
    }


HEADER = (
    "/* === DATA:START - generated by scripts/fetch_data.py. Do not edit by hand; ===\n"
    "   === edit manual.json for the read, the watch list and AI capex.        === */\n"
)
END = "/* === DATA:END === */"


def inject(data):
    path = ROOT / "index.html"
    html = path.read_text(encoding="utf8")
    a = html.find("/* === DATA:START")
    b = html.find(END)
    if a == -1 or b == -1:
        raise RuntimeError("DATA markers not found in index.html")
    block = HEADER + "var DATA = " + json.dumps(data, indent=2, ensure_ascii=False) + ";\n"
    path.write_text(html[:a] + block + html[b:], encoding="utf8")


def main():
    S = fetch_all()
    manual = json.loads((ROOT / "manual.json").read_text(encoding="utf8"))
    data = build(S, manual)
    inject(data)

    slope = data["curve"][2]["yield"] - data["curve"][1]["yield"]
    spread = data["oil"]["brent"]["price"] - data["oil"]["wti"]["price"]
    real_policy = data["fed"]["upper"] - data["inflation"]["cpi"]
    print(f"Built for {data['asOf']}")
    print(f"  10Y {data['curve'][2]['yield']}%   2s10s {r(slope)}")
    print(f"  CPI {data['inflation']['cpi']}%   PCE {data['inflation']['pce']}%")
    print(f"  HY {data['credit']['oas']}bp   Brent-WTI ${r(spread)}")
    print(f"  Debt/GDP {data['debt']['gdp']}%   real policy {r(real_policy)}")
    other = data["components"][5]
    print(f"  residual 'other'  CPI {other['yoy']}%   PCE {other['yoyPce']}%  (each ties to its own headline)")
    if abs(other["yoy"]) > 6 or abs(other["yoyPce"]) > 6:
        print("  NOTE: a large residual usually means the WEIGHTS constants are stale.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        sys.exit(f"Build failed, nothing written: {exc}")
