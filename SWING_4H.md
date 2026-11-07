# Swing 4H — evening pullback

Experimental long-only strategy on the existing 36-symbol US watchlist. No broker
orders and no Devil's Markers table reads/writes.

## Frozen rules

- US regular-session bars: 09:30–13:30 and 13:30–16:00 New York.
  Only the second fully closed bar confirms a new signal. Half days without
  this bar do not generate signals.
- Latest fully closed daily price above SMA200; signal price >= $5;
  20-session mean daily volume >= 1,000,000 shares.
- EMA20 > EMA50 on 4H. Previous close <= its EMA20. Confirmation close >
  its EMA20 and > previous high.
- Stop = minimum low of the last three bars minus 0.1 ATR14.
  Target = confirmation close + twice its initial distance to that stop.
  Levels remain frozen.
- Conditional paper entry at the next actual session open, preserving at
  least 1.5R. Late observations are MISSED; price outside the frozen band
  is SKIPPED_GAP. This simulated conditional opening fill is not a broker
  integration or a guarantee of an available fill.
- Known opening gaps execute first. Unknown intrabar ordering uses stop
  first, including on the entry bar. Time exit after 20 actual bars.
- Simulated $10,000, initial risk <= 0.5%, capital per symbol <= 20%,
  five positions, whole shares, no leverage. Alphabetical simultaneous
  entry priority. Intrabar exit proceeds cannot fund opening entries.
- Commission 5 bps + slippage 5 bps per side. Price returns only:
  dividends, cash interest, tax and financing excluded.

Breakout control: close above the previous 20-bar high; stop = close minus
2 ATR14. Same execution, liquidity and capital rules. It remains a research
comparison rather than an automatically selected paper replacement.

## Research

From scanner:

```powershell
py -3.12 -B swing_research.py --prepare-cache-from .local-backups/devils_sweep_cisd_v2/initial-4h/data --cache-dir .local-backups/swing-4h/initial/data --output .local-backups/swing-4h/initial/research.json
```

The target cache must be new. Preparation copies archived OHLC unchanged,
verifies overlapping daily prices against Yahoo, then attaches daily-volume
history. Missing cached files never silently fetch replacement prices.

Repeat with the augmented cache and a NEW immutable report path:

```powershell
py -3.12 -B swing_research.py --cache-dir .local-backups/swing-4h/initial/data --output .local-backups/swing-4h/recheck/research.json
```

Registration precedes evaluation. Reports include source/data hashes,
full/early/recent independent accounts, execution-cost stress and four
causal signal/portfolio prefixes per model. Open positions are marked to
market. Weekly statistics include flat weeks and label incomplete weeks.

The current fixed watchlist has selection/survivorship bias. The recent
period was already inspected; it is exploratory, not untouched validation.
IPOs need 200 daily bars to warm up. No weekly income or future profitability
is established.

## Separate forward paper

```powershell
py -3.12 -B swing_paper.py
py -3.12 -B swing_paper.py --persist
```

Default is dry-run. A new persisted account begins at the actual first run,
without historical fills. Only setups confirmed on the current New York
date can be newly observed. Subsequent runs preserve universe, configuration,
signal levels, entries, closed trades and cursor. Missing/revised source
candles for an open/pending trade stop the update instead of rewriting
history. Incomplete, stale or unsynchronized feeds fail the portfolio update.

Private Supabase Storage bucket `swing-4h` holds:

- `research/latest.json` and immutable timestamped research reports;
- `paper/latest.json`, immutable snapshots and `paper/health.json`.

Upload research with `swing_research.py --persist`. Existing service-role
credentials remain on the backend/Next.js server. Next authenticates before
reading private reports. No new database schema or public storage policy.

A create-if-absent `paper/write-lock.json` serializes writers. Only its owner
removes it. If a process crashes, verify no writer remains, inspect the object,
then explicitly remove that SINGLE lock; never empty the bucket. Immutable
snapshots support recovery. Failure preserves the previous account and writes
an error health status, hiding new actionable setups.

The separate `swing-4h.yml` runs after close using UTC DST candidates and a
New York evening guard. Local workflow changes do not activate remote Actions.

## Website

`/swing-4h`: evening setups, separate paper journal, historical comparison,
frozen rules. `/api/swing-4h` requires authentication and disables HTTP caching.
Signal confirmation and first observation are distinct timestamps.

The display gate hides pending suggestions at the next weekday 09:30 New
York; it is conservative on exchange holidays. Replay uses the next ACTUAL
session bar. Open positions stay in the journal. Health errors and market
data older than four days hide new suggestions while keeping history visible.

Charts use saved Yahoo candles from the same replay, original levels and
signal/entry/exit markers. Backtest rows appear under Results, never as new
paper setups. The first version does not enable external notifications or
broker automation.

## Checks

```powershell
py -3.12 -B -m pytest tests/test_swing_4h.py tests/test_swing_paper.py -q -p no:cacheprovider
```

From web: `node --test tests/swing-4h.test.cjs tests/devils-markers.test.cjs`;
`npx tsc --noEmit --incremental false`.
