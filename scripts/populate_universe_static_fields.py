#!/usr/bin/env python3
"""Populate static/provenance/review fields in data/universe.csv.

Conservative static pass: fills fields derivable from the existing Saxo/Yahoo columns,
applies obvious exclusion heuristics, and creates review queues. Writes the CSV after every
batch so progress is durable.
"""
from __future__ import annotations

import argparse, csv, json, re, shutil
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[1]
REPO_CSV = ROOT / "data" / "universe.csv"
LOCAL_CSV = Path("/Users/vivian/ISA-Autoresearch/universe.csv")
STATE_FILE = ROOT / "docs" / "universe_static_population_state.json"
BACKUP_DIR = Path("/Users/vivian/ISA-Autoresearch/universe-backups")

SOURCE_HMRC_RECOGNISED_EXCHANGES = (
    "GOV.UK recognised stock exchanges / designated countries; operator exchange-id mapping; "
    "listed/admitted status not independently determined"
)
HMRC_RECOGNISED_EXCHANGE_IDS = {
    "NASDAQ", "NYSE", "HKEX", "TYO", "ASX", "TSE", "PAR", "FSE", "XETR_ETF",
    "MIL_ETF", "LSE_ETF", "SWX_ETF", "LSE_SETS", "SSE", "AMS", "MIL", "OSE",
    "SIBE", "HSE", "CSE", "BRU", "VIE", "LISB", "VX", "ISE",
}
ADDED_COLUMNS = [
    "market_cap_source", "market_cap_checked_at", "aum_usd", "aum_source", "aum_checked_at",
    "hmrc_exchange_source", "hmrc_exchange_checked_at", "eligibility_review_status",
    "eligibility_review_notes",
]
REVIEW_COLUMNS = [
    "saxo_symbol", "yFinance_symbol", "description", "exchange_id", "asset_type", "currency",
    "issuer_country", "eligibility_status", "availability_status", "eligible_universe",
    "isa_eligible", "instrument_exclusion_flag", "market_cap_usd", "aum_usd", "ucits",
    "fund_structure", "leveraged", "inverse", "crypto_etn_or_etp", "money_market_or_cash_like",
    "eligibility_review_status", "eligibility_review_notes",
]
LEVERAGED_RE = re.compile(r"\b(leveraged|daily\s+long|ultra|\b2x\b|\b3x\b|\b4x\b|\b5x\b|\bx2\b|\bx3\b)\b", re.I)
INVERSE_RE = re.compile(r"\b(short|inverse|bear|\-1x|\-2x|\-3x|daily\s+short)\b", re.I)
CRYPTO_RE = re.compile(r"\b(bitcoin|ethereum|crypto|blockchain|\bbtc\b|\beth\b|solana|ripple|xrp)\b", re.I)
MONEY_MARKET_RE = re.compile(r"\b(money\s+market|cash|liquidity|overnight|t-?bill|treasury\s+bills?)\b", re.I)
RECEIPT_RE = re.compile(r"\b(adr|ads|gdr|depositary|depository)\b", re.I)
REDUCE_ONLY_RE = re.compile(r"\breduce\s+only\b", re.I)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def today() -> str:
    return date.today().isoformat()


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
    out = add_after(out, "market_cap_usd", ["market_cap_source", "market_cap_checked_at"])
    out = add_after(out, "market_cap_checked_at", ["aum_usd", "aum_source", "aum_checked_at"])
    out = add_after(out, "hmrc_recognised_exchange", ["hmrc_exchange_source", "hmrc_exchange_checked_at"])
    out = add_after(out, "eligibility_notes", ["eligibility_review_status", "eligibility_review_notes"])
    for col in ADDED_COLUMNS:
        if col not in out:
            out.append(col)
    return out


def set_if_blank(row: dict, key: str, value: str) -> None:
    if not str(row.get(key, "")).strip():
        row[key] = value


def is_stock(row: dict) -> bool:
    return str(row.get("asset_type", "")).strip().lower() in {"stock", "equity", "ordinary_share"}


def is_etf(row: dict) -> bool:
    return str(row.get("asset_type", "")).strip().lower() in {"etf", "fund", "exchange_traded_fund"}


def derive_fund_structure(description: str, row_is_etf: bool) -> str:
    d = description.upper()
    if " ETN" in d or d.endswith("ETN"):
        return "ETN"
    if " ETC" in d or d.endswith("ETC"):
        return "ETC"
    if " ETP" in d or d.endswith("ETP"):
        return "ETP"
    if row_is_etf or " ETF" in d or d.endswith("ETF"):
        return "ETF"
    return "N/A"


def append_note(notes: list[str], note: str) -> None:
    if note and note not in notes:
        notes.append(note)


def classify_row(row: dict, run_date: str) -> None:
    description = str(row.get("description") or row.get("name") or "")
    exchange_id = str(row.get("exchange_id") or row.get("exchange") or "")
    issuer_country = str(row.get("issuer_country") or row.get("country") or "")
    row_is_stock, row_is_etf = is_stock(row), is_etf(row)

    set_if_blank(row, "data_vendor_symbol", str(row.get("yFinance_symbol", "")))
    set_if_blank(row, "domicile", issuer_country or "NEEDS_CHECK")
    set_if_blank(row, "country", issuer_country or "NEEDS_CHECK")
    set_if_blank(row, "price_currency", str(row.get("currency", "")))
    set_if_blank(row, "primary_listing", exchange_id)
    if str(row.get("currency", "")).upper() == "GBP" and not str(row.get("fx_pair_to_gbp", "")).strip():
        row["fx_pair_to_gbp"] = "GBP/GBP"

    if exchange_id in HMRC_RECOGNISED_EXCHANGE_IDS:
        row["hmrc_recognised_exchange"] = "true"
        row["hmrc_exchange_source"] = SOURCE_HMRC_RECOGNISED_EXCHANGES
    else:
        row["hmrc_recognised_exchange"] = "NEEDS_CHECK"
        row["hmrc_exchange_source"] = "No static HMRC exchange mapping for this Saxo exchange_id"
    row["hmrc_exchange_checked_at"] = run_date

    fund_structure = derive_fund_structure(description, row_is_etf)
    if row_is_stock:
        row["fund_structure"] = "N/A"
        row["ucits"] = "N/A"
        row["aum_source"] = "N/A_STOCK"
    elif row_is_etf:
        row["fund_structure"] = fund_structure
        row["ucits"] = "true" if "UCITS" in description.upper() else "NEEDS_CHECK"
        set_if_blank(row, "aum_source", "NEEDS_PROVIDER")
    else:
        set_if_blank(row, "fund_structure", "NEEDS_CHECK")
        set_if_blank(row, "ucits", "NEEDS_CHECK")
        set_if_blank(row, "aum_source", "NEEDS_CHECK")

    if row_is_stock and not str(row.get("market_cap_usd", "")).strip():
        row["market_cap_source"] = "NEEDS_YFINANCE_OR_MARKET_DATA_PROVIDER"
        row["market_cap_checked_at"] = ""
    elif row_is_etf:
        set_if_blank(row, "market_cap_source", "N/A_ETF_USE_AUM")
        set_if_blank(row, "aum_source", "NEEDS_PROVIDER")

    # Leveraged/inverse/crypto/cash-like keyword exclusions are applied only to ETF/fund-like
    # rows. Stock names can legitimately contain words like "Ultra" or "Bear".
    leveraged = row_is_etf and bool(LEVERAGED_RE.search(description))
    inverse = row_is_etf and bool(INVERSE_RE.search(description))
    fund_struct = str(row.get("fund_structure", "")).upper()
    crypto = row_is_etf and bool(CRYPTO_RE.search(description)) and fund_struct in {"ETF", "ETN", "ETC", "ETP"}
    money_market = row_is_etf and bool(MONEY_MARKET_RE.search(description))
    receipt = bool(RECEIPT_RE.search(description))
    reduce_only = bool(REDUCE_ONLY_RE.search(description))
    row["leveraged"] = "true" if leveraged else "false"
    row["inverse"] = "true" if inverse else "false"
    row["crypto_etn_or_etp"] = "true" if crypto else "false"
    row["money_market_or_cash_like"] = "true" if money_market else "false"

    notes: list[str] = []
    status = "AUTO_PASS_STATIC_CHECKS"
    hard_excluded = leveraged or inverse or crypto or money_market or fund_struct in {"ETN", "ETC", "ETP"}
    if hard_excluded:
        row["instrument_exclusion_flag"] = "true"
        row["eligible_universe"] = "false"
        row["eligibility_status"] = "EXCLUDED"
        row["isa_eligible"] = "false"
        status = "AUTO_EXCLUDE"
        if leveraged:
            append_note(notes, "leveraged product keyword detected")
        if inverse:
            append_note(notes, "inverse/short product keyword detected")
        if crypto:
            append_note(notes, "crypto ETN/ETP/ETF keyword detected")
        if money_market:
            append_note(notes, "money-market/cash-like fund keyword detected")
        if fund_struct in {"ETN", "ETC", "ETP"}:
            append_note(notes, f"fund_structure={fund_struct} is outside equity/ordinary ETF mandate")
    else:
        # Reset baseline fields on every run so corrected heuristics can undo earlier
        # AUTO_EXCLUDE/NEEDS_* classifications. Review conditions below may tighten again.
        row["instrument_exclusion_flag"] = "false"
        row["eligible_universe"] = "true"
        row["eligibility_status"] = "OPERATOR_APPROVED"
        row["isa_eligible"] = "true"
        if not str(row.get("availability_status", "")).strip() or row.get("availability_status") == "NEEDS_CHECK":
            row["availability_status"] = "AVAILABLE"

    if not hard_excluded and receipt:
        row["eligibility_status"] = "NEEDS_UNDERLYING_CHECK"
        row["isa_eligible"] = "NEEDS_CHECK"
        row["eligible_universe"] = "false"
        status = "NEEDS_MANUAL_REVIEW"
        append_note(notes, "depositary receipt/ADR/ADS/GDR keyword detected; underlying-share ISA eligibility requires review")
    if not hard_excluded and reduce_only:
        row["availability_status"] = "NEEDS_CHECK"
        row["eligible_universe"] = "false"
        status = "NEEDS_MANUAL_REVIEW"
        append_note(notes, "description says Reduce Only; buy availability requires review")
    if not hard_excluded and row_is_etf and row.get("ucits") != "true":
        row["eligibility_status"] = "NEEDS_FUND_RECOGNITION_CHECK"
        row["isa_eligible"] = "NEEDS_CHECK"
        row["eligible_universe"] = "false"
        status = "NEEDS_MANUAL_REVIEW"
        append_note(notes, "ETF/fund row does not explicitly contain UCITS; fund recognition requires review")
    if not hard_excluded and row.get("hmrc_recognised_exchange") != "true":
        row["eligibility_status"] = "NEEDS_EXCHANGE_CHECK"
        row["isa_eligible"] = "NEEDS_CHECK"
        row["eligible_universe"] = "false"
        status = "NEEDS_MANUAL_REVIEW"
        append_note(notes, "HMRC recognised-exchange static mapping missing")
    if not hard_excluded and row_is_stock and not str(row.get("market_cap_usd", "")).strip():
        if status == "AUTO_PASS_STATIC_CHECKS":
            status = "STATIC_PASS_PENDING_MARKET_CAP"
        append_note(notes, "market_cap_usd still needs market-data enrichment; operator says sub-US$5bn equities removed upstream")
    if not hard_excluded and row_is_etf and not str(row.get("aum_usd", "")).strip():
        if status == "AUTO_PASS_STATIC_CHECKS":
            status = "STATIC_PASS_PENDING_AUM"
        append_note(notes, "aum_usd still needs provider enrichment if ETF size policy is introduced")
    if not str(row.get("isin", "")).strip():
        append_note(notes, "isin still needs OpenFIGI/provider enrichment")
    if not str(row.get("eligibility_checked_at", "")).strip():
        row["eligibility_checked_at"] = run_date
    row["eligibility_review_status"] = status
    row["eligibility_review_notes"] = "; ".join(notes) if notes else "Static checks passed; no obvious exclusion keyword detected"


def write_csv(path: Path, fields: List[str], rows: List[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(path)


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def write_review_csv(name: str, rows: List[dict]) -> None:
    with (ROOT / "docs" / name).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=REVIEW_COLUMNS, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def generate_review_queues(rows: List[dict]) -> None:
    write_review_csv("universe_review_exclusions.csv", [r for r in rows if r.get("eligibility_review_status") == "AUTO_EXCLUDE"])
    write_review_csv("universe_review_needs_check.csv", [r for r in rows if r.get("eligibility_review_status") == "NEEDS_MANUAL_REVIEW" or str(r.get("eligibility_status", "")).startswith("NEEDS") or str(r.get("availability_status", "")).startswith("NEEDS")])
    write_review_csv("universe_review_missing_market_cap.csv", [r for r in rows if is_stock(r) and not str(r.get("market_cap_usd", "")).strip()])
    write_review_csv("universe_review_adr_gdr.csv", [r for r in rows if RECEIPT_RE.search(str(r.get("description") or r.get("name") or ""))])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--max-batches", type=int, default=0)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--no-sync-local", action="store_true")
    args = ap.parse_args()
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    if args.reset or not STATE_FILE.exists():
        shutil.copy2(REPO_CSV, BACKUP_DIR / f"pre-static-populate.repo.universe.csv.{stamp}.bak")
        if LOCAL_CSV.exists():
            shutil.copy2(LOCAL_CSV, BACKUP_DIR / f"pre-static-populate.local.universe.csv.{stamp}.bak")
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
    run_date, n, next_index, batches_done = today(), len(rows), int(state.get("next_index", 0)), 0
    while next_index < n and (args.max_batches == 0 or batches_done < args.max_batches):
        end = min(next_index + args.batch_size, n)
        for row in rows[next_index:end]:
            classify_row(row, run_date)
        write_csv(REPO_CSV, fields, rows)
        if not args.no_sync_local:
            shutil.copy2(REPO_CSV, LOCAL_CSV)
        batch_status = Counter(r.get("eligibility_review_status", "") for r in rows[next_index:end])
        state["next_index"] = end
        state["updated_at"] = utc_now()
        state["batches"].append({"start": next_index, "end": end, "rows": end-next_index, "status_counts": dict(batch_status), "finished_at": utc_now()})
        save_state(state)
        print(f"batch {len(state['batches'])}: rows {next_index}-{end-1} {dict(batch_status)}", flush=True)
        next_index = end
        batches_done += 1
    if next_index >= n:
        generate_review_queues(rows)
        if not args.no_sync_local:
            shutil.copy2(REPO_CSV, LOCAL_CSV)
        print("review_queues_written=True")
        print("summary", dict(Counter(r.get("eligibility_review_status", "") for r in rows)))
    print(f"done_until={next_index} total={n} complete={next_index >= n}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
