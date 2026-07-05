#!/usr/bin/env python3
"""Map Saxo symbols in data/universe.csv to Yahoo Finance symbols.

The script is resumable and writes the CSV after each batch. It uses deterministic Yahoo
exchange suffix rules for full coverage and OpenFIGI only for likely Saxo-vs-exchange ticker
mismatches (for example RHMG:xetr -> RHM.DE), which keeps unauthenticated OpenFIGI usage small.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
REPO_CSV = ROOT / "data" / "universe.csv"
LOCAL_CSV = Path("/Users/vivian/ISA-Autoresearch/universe.csv")
STATE_FILE = ROOT / "docs" / "universe_yfinance_mapping_state.json"
BACKUP_DIR = Path("/Users/vivian/ISA-Autoresearch/universe-backups")

YAHOO_SUFFIX = {
    "xnas": "",      # NASDAQ
    "xnys": "",      # NYSE
    "xhkg": ".HK",   # Hong Kong
    "xtks": ".T",    # Tokyo
    "xasx": ".AX",   # Australia
    "xtse": ".TO",   # Toronto
    "xtsx": ".V",    # TSX Venture
    "xpar": ".PA",   # Paris
    "xams": ".AS",   # Amsterdam
    "xbru": ".BR",   # Brussels
    "xmil": ".MI",   # Milan
    "xetr": ".DE",   # XETRA / Germany on Yahoo
    "xfra": ".F",    # Frankfurt
    "xswx": ".SW",   # SIX Swiss
    "xvtx": ".SW",   # SIX Swiss
    "xome": ".ST",   # Stockholm
    "xcse": ".CO",   # Copenhagen
    "xngm": ".CO",   # current Nordic Growth row is Danish
    "xhel": ".HE",   # Helsinki
    "xosl": ".OL",   # Oslo
    "xmce": ".MC",   # Madrid
    "xwbo": ".VI",   # Vienna
    "xlis": ".LS",   # Lisbon
    "xdub": ".IR",   # Euronext Dublin
    "xlon": ".L",    # London Stock Exchange
}

# Bloomberg/OpenFIGI exchange codes used only for likely root corrections.
OPENFIGI_EXCH = {
    "xetr": "GY",
    "xwbo": "AV",
}

ADDED_COLUMNS = [
    "yFinance_symbol",
    "yFinance_mapping_status",
    "yFinance_mapping_source",
    "yFinance_mapping_notes",
    "openfigi_ticker",
    "openfigi_figi",
    "openfigi_name",
    "openfigi_exch_code",
    "openfigi_checked_at",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def split_saxo_symbol(saxo_symbol: str) -> Tuple[str, str]:
    if ":" in saxo_symbol:
        return tuple(saxo_symbol.split(":", 1))  # type: ignore[return-value]
    return saxo_symbol, ""


def ensure_columns(fields: List[str]) -> List[str]:
    out = list(fields)
    if "yFinance_symbol" not in out:
        insert_at = out.index("ticker") + 1 if "ticker" in out else len(out)
        out.insert(insert_at, "yFinance_symbol")
    for col in ADDED_COLUMNS:
        if col not in out:
            out.append(col)
    return out


def yahoo_base_transform(root: str, suffix: str) -> str:
    if suffix == "xhkg" and root.isdigit():
        # Yahoo HK uses no unnecessary leading zero, but at least four digits:
        # 00700 -> 0700, 09988 -> 9988.
        return str(int(root)).zfill(4)

    # Share classes in Yahoo commonly use hyphens: BRKb -> BRK-B, NOVOb -> NOVO-B.
    m = re.match(r"^([A-Z0-9]+)([a-z])$", root)
    if m:
        return f"{m.group(1)}-{m.group(2).upper()}"

    # Provider suffixes/classes commonly use underscores where Yahoo uses hyphens.
    root = root.replace("_", "-")
    root = re.sub(r"[a-z]+", lambda m: m.group(0).upper(), root)
    return root


def likely_openfigi_correction_candidates(root: str, suffix: str) -> List[str]:
    candidates = [root]
    if suffix == "xetr":
        if root.endswith("G") and len(root) > 2:
            candidates.append(root[:-1])
        if root.endswith("Gn") and len(root) > 3:
            candidates.append(root[:-2])
    elif suffix == "xwbo":
        if len(root) > 3 and root[-1] in {"V", "B"}:
            candidates.append(root[:-1])
    deduped: List[str] = []
    for c in candidates:
        if c and c not in deduped:
            deduped.append(c)
    return deduped


def should_use_openfigi(root: str, suffix: str) -> bool:
    if suffix == "xetr" and (root.endswith("G") or root.endswith("Gn")):
        return True
    if suffix == "xwbo" and len(root) > 3 and root[-1] in {"V", "B"}:
        return True
    return False


def openfigi_mapping(jobs: List[Tuple[Tuple[int, str], dict]], api_key: Optional[str]) -> Dict[Tuple[int, str], Optional[dict]]:
    if not jobs:
        return {}
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    max_jobs = 100 if api_key else 10
    sleep_seconds = 0.35 if api_key else 2.55
    out: Dict[Tuple[int, str], Optional[dict]] = {}
    for start in range(0, len(jobs), max_jobs):
        batch = jobs[start : start + max_jobs]
        body = json.dumps([job for _, job in batch]).encode()
        req = urllib.request.Request(
            "https://api.openfigi.com/v3/mapping",
            data=body,
            headers=headers,
            method="POST",
        )
        payload = None
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=45) as resp:
                    payload = json.loads(resp.read().decode())
                    break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 4:
                    time.sleep(8 * (attempt + 1))
                    continue
                raise
            except Exception:
                if attempt < 4:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise
        assert payload is not None
        for (key, job), res in zip(batch, payload):
            matches = []
            for item in res.get("data") or []:
                if (item.get("ticker") or "").upper() == str(job["idValue"]).upper():
                    matches.append(item)
            out[key] = matches[0] if matches else None
        if start + max_jobs < len(jobs):
            time.sleep(sleep_seconds)
    return out


def write_csv(path: Path, fields: List[str], rows: List[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"next_index": 0, "started_at": utc_now(), "batches": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-batches", type=int, default=0, help="0 = all remaining")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--no-sync-local", action="store_true")
    args = parser.parse_args()

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    if args.reset or not STATE_FILE.exists():
        shutil.copy2(REPO_CSV, BACKUP_DIR / f"pre-yfinance-batched.repo.universe.csv.{ts}.bak")
        if LOCAL_CSV.exists():
            shutil.copy2(LOCAL_CSV, BACKUP_DIR / f"pre-yfinance-batched.local.universe.csv.{ts}.bak")
        state = {"next_index": 0, "started_at": utc_now(), "batches": []}
        save_state(state)
    else:
        state = load_state()

    with REPO_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = ensure_columns(list(reader.fieldnames or []))

    # Ensure existing rows have every field.
    for row in rows:
        for field in fields:
            row.setdefault(field, "")

    n = len(rows)
    next_index = int(state.get("next_index", 0))
    batches_done = 0
    api_key = os.getenv("OPENFIGI_API_KEY")

    while next_index < n and (args.max_batches == 0 or batches_done < args.max_batches):
        end = min(next_index + args.batch_size, n)
        batch_rows = list(enumerate(rows[next_index:end], start=next_index))

        jobs: List[Tuple[Tuple[int, str], dict]] = []
        for idx, row in batch_rows:
            root, suffix = split_saxo_symbol(row.get("saxo_symbol", ""))
            if should_use_openfigi(root, suffix):
                for candidate in likely_openfigi_correction_candidates(root, suffix):
                    jobs.append(((idx, candidate), {
                        "idType": "TICKER",
                        "idValue": candidate,
                        "exchCode": OPENFIGI_EXCH[suffix],
                    }))

        figi = openfigi_mapping(jobs, api_key)
        checked_at = utc_now() if jobs else ""
        openfigi_corrections = 0
        unresolved_suffix = 0

        for idx, row in batch_rows:
            raw_root, suffix = split_saxo_symbol(row.get("saxo_symbol", ""))
            yahoo_suffix = YAHOO_SUFFIX.get(suffix)
            if yahoo_suffix is None:
                row["yFinance_symbol"] = ""
                row["yFinance_mapping_status"] = "UNRESOLVED"
                row["yFinance_mapping_source"] = "NO_SUFFIX_RULE"
                row["yFinance_mapping_notes"] = f"No Yahoo suffix rule for Saxo venue {suffix}"
                unresolved_suffix += 1
                continue

            chosen_root = raw_root
            figi_item = None
            if should_use_openfigi(raw_root, suffix):
                for candidate in likely_openfigi_correction_candidates(raw_root, suffix):
                    item = figi.get((idx, candidate))
                    if item:
                        chosen_root = candidate
                        figi_item = item
                        break

            base = yahoo_base_transform(chosen_root, suffix)
            row["yFinance_symbol"] = base + yahoo_suffix
            row["yFinance_mapping_status"] = "MAPPED"
            if figi_item:
                row["yFinance_mapping_source"] = "OPENFIGI_CORRECTED" if chosen_root != raw_root else "OPENFIGI_CONFIRMED"
                row["openfigi_ticker"] = figi_item.get("ticker", "")
                row["openfigi_figi"] = figi_item.get("figi", "")
                row["openfigi_name"] = figi_item.get("name", "")
                row["openfigi_exch_code"] = figi_item.get("exchCode", "")
                row["openfigi_checked_at"] = checked_at
                if chosen_root != raw_root:
                    openfigi_corrections += 1
                    row["yFinance_mapping_notes"] = f"OpenFIGI mapped Saxo root {raw_root} to exchange/Yahoo root {chosen_root}"
                else:
                    row["yFinance_mapping_notes"] = "OpenFIGI confirmed exchange ticker root"
            elif should_use_openfigi(raw_root, suffix):
                row["yFinance_mapping_source"] = "DETERMINISTIC_SUFFIX_OPENFIGI_NO_MATCH"
                row["yFinance_mapping_notes"] = "OpenFIGI did not confirm a root correction; deterministic Yahoo suffix applied"
                row["openfigi_checked_at"] = checked_at
            else:
                row["yFinance_mapping_source"] = "DETERMINISTIC_SUFFIX"
                row["yFinance_mapping_notes"] = "Deterministic Saxo venue to Yahoo suffix mapping"

        write_csv(REPO_CSV, fields, rows)
        if not args.no_sync_local:
            shutil.copy2(REPO_CSV, LOCAL_CSV)

        state["next_index"] = end
        state["updated_at"] = utc_now()
        state["batches"].append({
            "start": next_index,
            "end": end,
            "rows": end - next_index,
            "openfigi_jobs": len(jobs),
            "openfigi_corrections": openfigi_corrections,
            "unresolved_suffix": unresolved_suffix,
            "finished_at": utc_now(),
        })
        save_state(state)

        print(
            f"batch {len(state['batches'])}: rows {next_index}-{end-1} "
            f"openfigi_jobs={len(jobs)} corrections={openfigi_corrections} "
            f"unresolved_suffix={unresolved_suffix}",
            flush=True,
        )
        next_index = end
        batches_done += 1

    print(f"done_until={next_index} total={n} complete={next_index >= n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
