NOTE:

link index russell 2000 (scarica csv):

https://www.ishares.com/us/products/239710/ishares-russell-2000-etf

## Devil's Markers outcome integrity

The live radar uses the **last 3 closed candles** per timeframe (4H/1D/1W/1M),
regular US sessions and Yahoo split-adjusted prices **without dividend adjustment**.
The window counts candles, not days: a monthly pattern can be two months old.
Feed expiry uses the latest available bar, separately from the pattern's position.
Strategy version 2 requires open == low for a bullish close or open == high for a
bearish close; the equality tolerance only covers floating-point noise.

Each new signal saves its source OHLC, price basis, first live observation and radar
validation. Rescans hide revised candles, historical-only imports and patterns outside
the recent window. Stalled feeds expire (7 days for 4H/daily, 14 for weekly, 45 for
monthly). Radar eligibility is separate from a simulated trade's open/closed state:
ageing out never invents a trade outcome. Historical backfills cannot overwrite saved
simulations. The scheduled scanner skips historical backfills.

```powershell
py -3.12 -B devils_scanner.py --recent-candles 3 --skip-backfill
```

Resolver version 3 processes opening fills before intrabar touches. When the open
is strictly between SL and TP and both are touched, the result remains a
conservative loss and `meta.execution.ambiguous_exit_bar` is true. Weekly/monthly
trades use closed daily bars after the signal close, including the next forming
week/month. `meta.execution.resolution_timeframe` and the outcome's matching field
identify daily resolution; `bars_held` then counts daily bars.

New closed rows include `closed_at` (the exit **bar label**, not a tick timestamp),
`meta.execution.exit_price` and the versioned outcome excursions. Entries remain
theoretical signal-candle closes, identified by `meta.entry_basis = signal_close`.
`meta.signal_available_at` follows the existing session calendar; it is distinct
from `meta.first_seen_at`. Reconstructed historical signals are tagged separately
from signals first observed open by the scanner.

Apply `migrations/20261107_devils_closed_time_and_authenticated_read.sql` in the
Supabase SQL Editor to allow paper time exits and restrict signal reads to signed-in
users. Committing or pushing this repository does not apply that migration.

Historical rows are not silently rewritten by the scanner. Preview repairs first:

```powershell
py -3.12 backfill_r_multiple.py --dry-run --repair-outcomes --limit 50
```

The same command without `--dry-run` writes the repaired R, fill, exit bar time and
excursions. `--repair-outcomes` explicitly permits changed WIN/LOSS labels; without
it, those rows are reported as mismatches. A missing entry bar leaves a row untouched.
This requires the service-role credential and existing historical market data.
Repair tools skip rows without the current documented price basis: legacy adjusted
levels cannot be safely replayed against today's non-dividend-adjusted candles.

Offline regression checks, from this repository's root:

```powershell
py -3.12 -B -m pytest -q -p no:cacheprovider tests/test_devils_markers.py tests/test_devils_outcome_regression.py tests/test_gap_fill.py tests/test_devils_data_integrity.py tests/test_signal_outcome.py tests/test_optimizer_validation.py
```

The legacy CRT optimizer is separate from Devils. Parameter search now reserves the
last 20% of daily history, and code changes require paired held-out evidence across
at least three tickers, at least 30 closed trades per side/ticker, positive candidate
expectancy after estimated costs and the configured improvement threshold. An open
held-out trade or insufficient evidence blocks promotion. This is not a trained
Devils scoring model or a complete walk-forward research pipeline.
