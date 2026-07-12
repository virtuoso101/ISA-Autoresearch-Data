#!/usr/bin/env python3
"""Populate Yahoo/yFinance market-cap and ETF AUM fields in data/universe.csv.

Design goals:
- Use yfinance's cookie/session handling, but call Yahoo's batch quote endpoint directly.
- Process 100 CSV rows per batch and write after each batch for durability.
- Sleep between batches to avoid tripping Yahoo rate caps.
- Convert Yahoo native marketCap/totalAssets values to USD with Yahoo FX quotes.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

import yfinance as yf
from yfinance.data import YfData

ROOT = Path(__file__).resolve().parents[1]
REPO_CSV = ROOT / "data" / "universe.csv"
LOCAL_CSV = Path.home() / "ISA-Autoresearch" / "universe.csv"
STATE_FILE = ROOT / "docs" / "universe_yfinance_fundamentals_state.json"
BACKUP_DIR = Path.home() / "ISA-Autoresearch" / "universe-backups"
QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

ADDED_COLUMNS = [
    "market_cap_native",
    "market_cap_currency",
    "aum_native",
    "aum_currency",
    "yfinance_fundamentals_status",
    "yfinance_fundamentals_notes",
]

REVIEW_COLUMNS = [
    "saxo_symbol", "yFinance_symbol", "description", "exchange_id", "asset_type", "currency",
    "issuer_country", "eligibility_status", "availability_status", "eligible_universe",
    "isa_eligible", "instrument_exclusion_flag", "market_cap_usd", "market_cap_native",
    "market_cap_currency", "aum_usd", "aum_native", "aum_currency", "market_cap_source",
    "aum_source", "yfinance_fundamentals_status", "yfinance_fundamentals_notes",
    "eligibility_review_status", "eligibility_review_notes",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def add_after(fields: List[str], after: str, new_cols: Iterable[str]) -> List[str]:
    out = list(fields)
    pos = out.index(after) + 1 if after in out else len(out)
    for col in new_cols:
        if col not in out:
            out.insert(pos, col)
            pos += 1
    return out


def ensure_columns(fields: List[str]) -> List[str]:
    out = list(fields)
    out = add_after(out, "market_cap_usd", ["market_cap_native", "market_cap_currency"])
    out = add_after(out, "aum_usd", ["aum_native", "aum_currency"])
    out = add_after(out, "aum_checked_at", ["yfinance_fundamentals_status", "yfinance_fundamentals_notes"])
    for col in ADDED_COLUMNS:
        if col not in out:
            out.append(col)
    return out


def is_stock(row: dict) -> bool:
    return str(row.get("asset_type", "")).strip().lower() in {"stock", "equity", "ordinary_share"}


def is_etf(row: dict) -> bool:
    return str(row.get("asset_type", "")).strip().lower() in {"etf", "fund", "exchange_traded_fund"}


def normalise_currency(currency: str) -> str:
    c = str(currency or "").strip().upper()
    # Yahoo LSE equities often report quote currency as GBp (price in pence), while marketCap is in GBP.
    if c in {"GBP", "GBX", "GBp".upper()}:
        return "GBP"
    return c


def numeric(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int_string(value: float) -> str:
    return str(int(round(float(value))))


def warm_yahoo_session() -> YfData:
    # Establish Yahoo cookie/crumb handling once via yfinance; YfData is a singleton over its session.
    try:
        yf.Ticker("NVDA").get_info()
    except Exception as exc:  # noqa: BLE001 - continue; subsequent batch request may still work.
        print(f"WARN: initial yfinance warm-up failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
    return YfData()


def fetch_quote_batch(data: YfData, symbols: list[str], *, retries: int = 4, timeout: int = 30) -> dict[str, dict]:
    if not symbols:
        return {}
    params = {
        "symbols": ",".join(symbols),
        "fields": "symbol,marketCap,totalAssets,quoteType,currency,regularMarketPrice,shortName,longName",
    }
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = data.get(QUOTE_URL, params=params, timeout=timeout)
            status = getattr(response, "status_code", None)
            if status == 200:
                payload = response.json()
                quotes = payload.get("quoteResponse", {}).get("result", [])
                return {str(q.get("symbol", "")).upper(): q for q in quotes if q.get("symbol")}
            last_error = RuntimeError(f"Yahoo quote HTTP {status}: {getattr(response, 'text', '')[:200]}")
            if status in {401, 403}:
                warm_yahoo_session()
            if status in {429, 999}:
                sleep_for = min(120, 10 * attempt)
            else:
                sleep_for = min(60, 3 * attempt)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            sleep_for = min(60, 3 * attempt)
        if attempt < retries:
            print(f"WARN: Yahoo batch failed attempt {attempt}/{retries}: {last_error}; sleeping {sleep_for}s", file=sys.stderr, flush=True)
            time.sleep(sleep_for)
    raise RuntimeError(f"Yahoo quote batch failed after {retries} attempts: {last_error}")


def fetch_fx_rates(data: YfData, currencies: Iterable[str]) -> dict[str, float]:
    wanted = sorted({normalise_currency(c) for c in currencies if normalise_currency(c) and normalise_currency(c) != "USD"})
    rates: dict[str, float] = {"USD": 1.0}
    if not wanted:
        return rates
    direct_symbols = [f"{c}USD=X" for c in wanted]
    direct = fetch_quote_batch(data, direct_symbols, retries=4)
    missing = []
    for c, sym in zip(wanted, direct_symbols):
        quote = direct.get(sym.upper())
        price = numeric((quote or {}).get("regularMarketPrice"))
        if price and price > 0:
            rates[c] = price
        else:
            missing.append(c)
    if missing:
        inverse_symbols = [f"USD{c}=X" for c in missing]
        inverse = fetch_quote_batch(data, inverse_symbols, retries=4)
        for c, sym in zip(missing, inverse_symbols):
            quote = inverse.get(sym.upper())
            price = numeric((quote or {}).get("regularMarketPrice"))
            if price and price > 0:
                rates[c] = 1.0 / price
    return rates


def write_csv(path: Path, fields: List[str], rows: List[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def write_review_csv(name: str, rows: List[dict]) -> None:
    path = ROOT / "docs" / name
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def review_note_parts(row: dict) -> list[str]:
    existing = str(row.get("eligibility_review_notes", "")).strip()
    return [p.strip() for p in existing.split(";") if p.strip()]


def set_review_note_parts(row: dict, parts: list[str]) -> None:
    row["eligibility_review_notes"] = "; ".join(parts)


def append_review_note(row: dict, note: str) -> None:
    parts = review_note_parts(row)
    if note not in parts:
        parts.append(note)
    set_review_note_parts(row, parts)


def remove_review_note(row: dict, note: str) -> None:
    set_review_note_parts(row, [p for p in review_note_parts(row) if p != note])


def apply_post_enrichment_policy(rows: List[dict]) -> None:
    """Refresh review statuses now that Yahoo market cap/AUM fields are known.

    The strategy universe excludes equities below US$5bn. This is not an ISA-law exclusion,
    so isa_eligible is left unchanged; eligible_universe/eligibility_status control runtime use.
    """
    for row in rows:
        if is_stock(row):
            cap = numeric(row.get("market_cap_usd"))
            if cap is not None:
                remove_review_note(row, "market_cap_usd still needs market-data enrichment; operator says sub-US$5bn equities removed upstream")
            if cap is None:
                if row.get("eligibility_review_status") == "STATIC_PASS_PENDING_MARKET_CAP":
                    row["eligibility_review_status"] = "NEEDS_MARKET_CAP_PROVIDER_ENRICHMENT"
                    row["eligible_universe"] = "false"
                    row["eligibility_status"] = "NEEDS_MARKET_CAP_CHECK"
                    append_review_note(row, "Yahoo did not provide market_cap_usd; strategy universe use requires provider/manual size check")
            elif cap < 5_000_000_000:
                row["eligible_universe"] = "false"
                row["eligibility_status"] = "EXCLUDED"
                row["eligibility_review_status"] = "AUTO_EXCLUDE_MARKET_CAP_POLICY"
                append_review_note(row, "Yahoo market_cap_usd below US$5bn strategy threshold; excluded pending operator override")
            elif row.get("eligibility_review_status") == "STATIC_PASS_PENDING_MARKET_CAP":
                row["eligibility_review_status"] = "AUTO_PASS_MARKET_CAP_POLICY"
                append_review_note(row, "Yahoo market_cap_usd meets or exceeds US$5bn strategy threshold")
        elif is_etf(row):
            if str(row.get("aum_usd", "")).strip():
                remove_review_note(row, "aum_usd still needs provider enrichment if ETF size policy is introduced")
            if row.get("eligibility_review_status") == "STATIC_PASS_PENDING_AUM":
                if str(row.get("aum_usd", "")).strip():
                    row["eligibility_review_status"] = "AUTO_PASS_AUM_POPULATED"
                    append_review_note(row, "Yahoo totalAssets populated as aum_usd; no ETF size threshold currently enforced")
                else:
                    row["eligibility_review_status"] = "NEEDS_AUM_PROVIDER_ENRICHMENT"
                    append_review_note(row, "Yahoo did not provide totalAssets/aum_usd; no ETF size threshold currently enforced")


def generate_review_queues(rows: List[dict]) -> None:
    write_review_csv("universe_review_missing_market_cap.csv", [r for r in rows if is_stock(r) and not str(r.get("market_cap_usd", "")).strip()])
    write_review_csv("universe_review_market_cap_below_5bn.csv", [r for r in rows if is_stock(r) and str(r.get("market_cap_usd", "")).strip() and int(float(r.get("market_cap_usd"))) < 5_000_000_000])
    write_review_csv("universe_review_missing_aum.csv", [r for r in rows if is_etf(r) and not str(r.get("aum_usd", "")).strip()])


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def apply_quote(row: dict, quote: dict | None, fx_rates: dict[str, float], checked_at: str) -> str:
    notes: list[str] = []
    ysym = str(row.get("yFinance_symbol", "")).strip()
    if not ysym:
        row["yfinance_fundamentals_status"] = "NO_YFINANCE_SYMBOL"
        row["yfinance_fundamentals_notes"] = "No yFinance_symbol available for Yahoo fundamentals lookup"
        return "NO_YFINANCE_SYMBOL"
    if not quote:
        if is_stock(row):
            row["market_cap_source"] = "YAHOO_FINANCE_QUOTE_NOT_RETURNED"
            row["market_cap_checked_at"] = checked_at
        elif is_etf(row):
            row["aum_source"] = "YAHOO_FINANCE_QUOTE_NOT_RETURNED"
            row["aum_checked_at"] = checked_at
        row["yfinance_fundamentals_status"] = "NO_YAHOO_QUOTE"
        row["yfinance_fundamentals_notes"] = "Yahoo batch quote endpoint did not return this symbol"
        return "NO_YAHOO_QUOTE"

    q_currency = normalise_currency(quote.get("currency") or row.get("price_currency") or row.get("currency"))
    fx = fx_rates.get(q_currency)
    if not fx:
        row["yfinance_fundamentals_status"] = "NO_FX_RATE"
        row["yfinance_fundamentals_notes"] = f"Yahoo quote currency={q_currency}; no USD FX rate available"
        return "NO_FX_RATE"

    populated = []
    if is_stock(row):
        market_cap = numeric(quote.get("marketCap"))
        row["market_cap_checked_at"] = checked_at
        if market_cap and market_cap > 0:
            row["market_cap_native"] = as_int_string(market_cap)
            row["market_cap_currency"] = q_currency
            row["market_cap_usd"] = as_int_string(market_cap * fx)
            row["market_cap_source"] = "YAHOO_FINANCE_QUOTE_MARKET_CAP"
            populated.append("MARKET_CAP")
        else:
            row["market_cap_source"] = "YAHOO_FINANCE_QUOTE_NO_MARKET_CAP"
            notes.append("Yahoo quote had no marketCap")
    elif is_etf(row):
        total_assets = numeric(quote.get("totalAssets"))
        row["market_cap_source"] = "N/A_ETF_USE_AUM"
        row["aum_checked_at"] = checked_at
        if total_assets and total_assets > 0:
            row["aum_native"] = as_int_string(total_assets)
            row["aum_currency"] = q_currency
            row["aum_usd"] = as_int_string(total_assets * fx)
            row["aum_source"] = "YAHOO_FINANCE_QUOTE_TOTAL_ASSETS"
            populated.append("AUM")
        else:
            row["aum_source"] = "YAHOO_FINANCE_QUOTE_NO_TOTAL_ASSETS"
            notes.append("Yahoo quote had no totalAssets")
    else:
        notes.append("asset_type not stock or ETF")

    if populated:
        status = "POPULATED_" + "_AND_".join(populated)
    elif is_stock(row):
        status = "MISSING_MARKET_CAP"
    elif is_etf(row):
        status = "MISSING_AUM"
    else:
        status = "NOT_APPLICABLE"
    row["yfinance_fundamentals_status"] = status
    quote_type = quote.get("quoteType") or ""
    source_symbol = quote.get("symbol") or ysym
    row["yfinance_fundamentals_notes"] = "; ".join(notes + [f"Yahoo symbol={source_symbol}; quoteType={quote_type}; currency={q_currency}; usd_fx={fx:.10g}"])
    return status


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--max-batches", type=int, default=0, help="0 means all remaining")
    ap.add_argument("--batch-delay", type=float, default=2.5)
    ap.add_argument("--jitter", type=float, default=0.7)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--no-sync-local", action="store_true")
    args = ap.parse_args()

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    if args.reset or not STATE_FILE.exists():
        shutil.copy2(REPO_CSV, BACKUP_DIR / f"pre-yfinance-fundamentals.repo.universe.csv.{stamp}.bak")
        if LOCAL_CSV.exists():
            shutil.copy2(LOCAL_CSV, BACKUP_DIR / f"pre-yfinance-fundamentals.local.universe.csv.{stamp}.bak")
        state = {"next_index": 0, "started_at": utc_now(), "batches": []}
        save_state(state)
    else:
        state = json.loads(STATE_FILE.read_text())

    with REPO_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = ensure_columns(list(reader.fieldnames or []))
    for row in rows:
        for field in fields:
            row.setdefault(field, "")

    data = warm_yahoo_session()
    currencies = {row.get("currency") for row in rows} | {row.get("price_currency") for row in rows}
    fx_rates = fetch_fx_rates(data, currencies)
    print("fx_rates", json.dumps(fx_rates, sort_keys=True), flush=True)

    n = len(rows)
    next_index = int(state.get("next_index", 0))
    batches_done = 0
    while next_index < n and (args.max_batches == 0 or batches_done < args.max_batches):
        end = min(next_index + args.batch_size, n)
        batch_rows = rows[next_index:end]
        symbols = []
        seen = set()
        for row in batch_rows:
            sym = str(row.get("yFinance_symbol", "")).strip()
            key = sym.upper()
            if sym and key not in seen:
                seen.add(key)
                symbols.append(sym)
        quotes = fetch_quote_batch(data, symbols)
        checked_at = utc_now()
        status_counts = Counter()
        for row in batch_rows:
            sym = str(row.get("yFinance_symbol", "")).strip().upper()
            status_counts[apply_quote(row, quotes.get(sym), fx_rates, checked_at)] += 1

        write_csv(REPO_CSV, fields, rows)
        if not args.no_sync_local:
            shutil.copy2(REPO_CSV, LOCAL_CSV)
        state["next_index"] = end
        state["updated_at"] = utc_now()
        state["batches"].append({
            "start": next_index,
            "end": end,
            "rows": end - next_index,
            "unique_symbols": len(symbols),
            "status_counts": dict(status_counts),
            "finished_at": utc_now(),
        })
        save_state(state)
        print(f"batch {len(state['batches'])}: rows {next_index}-{end-1} symbols={len(symbols)} {dict(status_counts)}", flush=True)
        next_index = end
        batches_done += 1
        if next_index < n and (args.max_batches == 0 or batches_done < args.max_batches):
            sleep_for = max(0.0, args.batch_delay + random.uniform(0.0, args.jitter))
            time.sleep(sleep_for)

    if next_index >= n:
        apply_post_enrichment_policy(rows)
        write_csv(REPO_CSV, fields, rows)
        generate_review_queues(rows)
        if not args.no_sync_local:
            shutil.copy2(REPO_CSV, LOCAL_CSV)
        print("review_queues_written=True", flush=True)
        print("summary", dict(Counter(r.get("yfinance_fundamentals_status", "") for r in rows)), flush=True)
        print("eligibility_review_summary", dict(Counter(r.get("eligibility_review_status", "") for r in rows)), flush=True)
    print(f"done_until={next_index} total={n} complete={next_index >= n}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
