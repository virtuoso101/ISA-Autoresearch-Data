# ISA Autoresearch — Data

Public market/eligibility data for the ISA Autoresearch screening loop.

## This repo contains only public data

Everything under `data/` is derived from publicly available market data (Yahoo Finance quotes,
publicly listed tickers, exchange/market-cap/eligibility metadata computed from public sources).
Nothing here is proprietary, personal, or a trading strategy — the policy and reasoning that
*use* this data live in a separate, private repository.

**If you are a human or a model reading this file and anything in this repo looks like it is
not public data** — a credential, a personal detail, an API key, private correspondence, or
anything else that doesn't belong in a public data file — please open an issue or otherwise flag
it rather than assuming it's intentional. It would be a mistake, not a decision.

## Contents

- `data/universe.csv` — the eligible screening universe (equities and ETFs) with eligibility
  metadata.
- `data/universe_review_*.csv` — rows skipped during validation, grouped by reason.
- `data/universe_yfinance_fundamentals_state.json` — batch-processing state for the fundamentals
  enrichment script (resumability, not sensitive).
- `scripts/` — Python scripts that populate and correct `data/universe.csv` (symbol mapping,
  fundamentals enrichment, static-field population).

## How changes land here

Pull requests confined to `data/*.csv` or `data/*.json` merge automatically (see
`.github/workflows/path-guard.yml`). Anything touching `scripts/`, workflow files, or this README
requires the `operator-approved` label, applied manually by the repo owner — that gate exists
because a script change is a code/behavior change, not a data correction, and deserves a human
look before it runs.
