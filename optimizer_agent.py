"""
CRT Flow Optimizer Agent

Runs autonomously (weekly cron) to:
  1. Pull LOSS trades from Supabase and analyze patterns
  2. Run backtester grid search on the top losing tickers
  3. Build a structured prompt with findings
  4. Invoke OpenCode AI to apply the optimal parameters to scanner.py

Usage:
    python optimizer_agent.py               # full run (invokes OpenCode)
    python optimizer_agent.py --dry-run     # analysis + prompt only, no OpenCode
    python optimizer_agent.py --tickers AAPL NVDA MSFT   # override tickers
"""
import argparse
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client

# Local import — backtester must be in the same directory
sys.path.insert(0, os.path.dirname(__file__))
from backtester import (
    ScannerParams, PARAM_GRID, MIN_TRADES,
    simulate, compute_stats, grid_search,
)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

LOSS_LIMIT             = 200   # max losses to pull from Supabase
TOP_TICKERS_N          = 5     # how many top-loss tickers to run grid search on
BACKTEST_PERIOD        = "5y"  # daily history for backtester
MIN_CONSENSUS_TICKERS  = 3     # abort recommendation if fewer tickers produce grid results
OPENCODE_CMD           = "opencode"  # or full path if not on PATH
IMPROVEMENT_THRESHOLD  = 0.05  # consensus params must beat baseline by ≥5% expectancy
REPO_ROOT              = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ─────────────────────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────────────────────

def setup_supabase():
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("Missing Supabase credentials in .env.local")
    return create_client(url, key)


def log_run(supabase, payload: dict):
    """
    Insert one row into optimizer_runs.
    Never raises — a logging failure must not crash the agent.
    """
    try:
        supabase.table("optimizer_runs").insert(payload).execute()
        print(f"[+] Run logged to Supabase (status={payload.get('status')}).")
    except Exception as e:
        print(f"[!] Could not log run to Supabase: {e}")


def pull_losses(supabase) -> list:
    """Pull recent LOSS trades from crt_signals."""
    res = (
        supabase.table("crt_signals")
        .select("symbol,type,timeframe,entry_price,stop_loss,take_profit,exit_reason,closed_at,result,liquidity_tier")
        .eq("result", "LOSS")
        .order("closed_at", desc=True)
        .limit(LOSS_LIMIT)
        .execute()
    )
    return res.data or []


# ─────────────────────────────────────────────────────────────
# LOSS PATTERN ANALYSIS
# ─────────────────────────────────────────────────────────────

def analyze_loss_patterns(losses: list) -> dict:
    """
    Produce a structured breakdown of why trades failed.
    Mirrors the categories that scanner.py records in exit_reason.
    """
    if not losses:
        return {}

    total = len(losses)

    # By exit reason
    exit_reasons = Counter(t.get("exit_reason") or "Unknown" for t in losses)

    # By direction
    directions = Counter(
        "bearish" if "bearish" in (t.get("type") or "") else "bullish"
        for t in losses
    )

    # By tier (liquidity_tier field — e.g. "PDH Sweep", "PWL Sweep")
    tiers = Counter(t.get("liquidity_tier") or "Unknown" for t in losses)

    # By timeframe
    timeframes = Counter(t.get("timeframe") or "Unknown" for t in losses)

    # SL distance stats
    sl_distances = []
    for t in losses:
        try:
            entry = float(t.get("entry_price") or 0)
            sl    = float(t.get("stop_loss") or 0)
            if entry > 0 and sl > 0:
                sl_distances.append(abs(entry - sl) / entry * 100)
        except (TypeError, ValueError):
            pass

    avg_sl_dist = round(sum(sl_distances) / len(sl_distances), 3) if sl_distances else 0

    # Top tickers by loss frequency
    ticker_freq = Counter(t.get("symbol") for t in losses)
    top_tickers = [sym for sym, _ in ticker_freq.most_common(TOP_TICKERS_N) if sym]

    return {
        "total_losses": total,
        "exit_reasons": dict(exit_reasons.most_common()),
        "by_direction": dict(directions),
        "by_tier": dict(tiers.most_common()),
        "by_timeframe": dict(timeframes.most_common()),
        "avg_sl_distance_pct": avg_sl_dist,
        "top_loss_tickers": top_tickers,
    }


def print_analysis(analysis: dict):
    print("\n" + "="*60)
    print("  LOSS ANALYSIS REPORT")
    print("="*60)
    print(f"  Total losses analysed : {analysis['total_losses']}")
    print(f"  Avg SL distance       : {analysis['avg_sl_distance_pct']:.3f}%")
    print(f"\n  By Exit Reason:")
    for reason, n in analysis["exit_reasons"].items():
        pct = round(n / analysis["total_losses"] * 100, 1)
        print(f"    {reason:35s}: {n:3d} ({pct}%)")
    print(f"\n  By Tier:")
    for tier, n in analysis["by_tier"].items():
        print(f"    {tier:20s}: {n}")
    print(f"\n  By Direction:")
    for d, n in analysis["by_direction"].items():
        print(f"    {d:10s}: {n}")
    print(f"\n  Top loss tickers: {', '.join(analysis['top_loss_tickers'])}")


# ─────────────────────────────────────────────────────────────
# GRID SEARCH ACROSS TOP TICKERS
# ─────────────────────────────────────────────────────────────

# Only optimize the 3 most impactful params by default.
# Use --full-grid to test all 6 (slower).
DEFAULT_GRID_PARAMS = ["wall_wick_pct", "fuel_wick_pct", "displacement_mult"]


def run_grid_search_on_tickers(
    tickers: list, full_grid: bool = False
) -> tuple[dict | None, dict]:
    """
    Run grid search on each ticker and aggregate the best params
    by consensus (most common value per param).
    Returns (consensus_params | None, data_cache).
    data_cache: {ticker: (daily_df, hourly_df)} — reused by validate_improvement.
    """
    param_keys = None if full_grid else DEFAULT_GRID_PARAMS
    all_best   = []
    data_cache = {}

    for ticker in tickers:
        print(f"\n[*] Downloading data for {ticker}...")
        try:
            obj       = yf.Ticker(ticker)
            daily_df  = obj.history(period=BACKTEST_PERIOD, interval="1d")
            hourly_df = obj.history(period="730d", interval="1h")
            daily_df.dropna(inplace=True)
            hourly_df.dropna(inplace=True)

            if len(daily_df) < 30 or len(hourly_df) < 100:
                print(f"  Insufficient data for {ticker}, skipping.")
                continue

            data_cache[ticker] = (daily_df, hourly_df)
            best = grid_search(ticker, daily_df, hourly_df, param_keys=param_keys)
            if best:
                all_best.append(best)
        except Exception as e:
            print(f"  Error on {ticker}: {e}")
            continue

    if not all_best:
        return None, data_cache

    if len(all_best) < MIN_CONSENSUS_TICKERS:
        contributing = [b.get("_ticker", "?") for b in all_best]
        print(f"\n[!] Only {len(all_best)}/{len(tickers)} tickers produced grid results "
              f"({', '.join(contributing)}).")
        print(f"    Minimum required for a reliable consensus: {MIN_CONSENSUS_TICKERS}.")
        print(f"    Aborting recommendation — sample too small to generalise.")
        print(f"    Suggestion: run with --tickers on larger/more liquid symbols (e.g. AAPL NVDA MSFT AMZN META).")
        return None, data_cache

    # Consensus: for each param, take the most common value
    contributing = [b.get("_ticker", "?") for b in all_best]
    print(f"\n[*] Building consensus from {len(all_best)} ticker(s): {', '.join(contributing)}")
    all_param_keys = list(PARAM_GRID.keys())
    consensus = {}
    for k in all_param_keys:
        values = [b[k] for b in all_best if k in b]
        if values:
            counter = Counter(values)
            consensus[k] = counter.most_common(1)[0][0]

    return consensus, data_cache


# ─────────────────────────────────────────────────────────────
# IMPROVEMENT GATE
# ─────────────────────────────────────────────────────────────

def validate_improvement(consensus: dict, data_cache: dict) -> tuple[bool, float, float]:
    """
    Runs simulate() with default params AND consensus params on every cached ticker.
    Returns (should_proceed, avg_baseline_expectancy, avg_consensus_expectancy).
    Aborts if improvement < IMPROVEMENT_THRESHOLD.
    """
    from backtester import simulate, compute_stats, ScannerParams, MIN_TRADES

    baseline_exps  = []
    consensus_exps = []
    defaults       = ScannerParams()
    consensus_p    = ScannerParams(**{k: v for k, v in consensus.items() if k in PARAM_GRID})

    print(f"\n[*] Validating consensus params vs baseline on {len(data_cache)} ticker(s)...")

    for ticker, (daily_df, hourly_df) in data_cache.items():
        # Baseline
        b_trades = simulate(ticker, daily_df, hourly_df, defaults)
        b_stats  = compute_stats(b_trades)
        if b_stats and b_stats["total"] >= MIN_TRADES:
            baseline_exps.append(b_stats["expectancy"])

        # Consensus
        c_trades = simulate(ticker, daily_df, hourly_df, consensus_p)
        c_stats  = compute_stats(c_trades)
        if c_stats and c_stats["total"] >= MIN_TRADES:
            consensus_exps.append(c_stats["expectancy"])

    if not baseline_exps or not consensus_exps:
        print("[!] Not enough trades to compare — skipping improvement gate.")
        return True, 0.0, 0.0

    avg_b = sum(baseline_exps) / len(baseline_exps)
    avg_c = sum(consensus_exps) / len(consensus_exps)
    # Relative improvement (handles negative baseline correctly)
    denom      = abs(avg_b) if abs(avg_b) > 0.001 else 0.001
    rel_improv = (avg_c - avg_b) / denom

    print(f"  Baseline  expectancy : {avg_b:+.4f} R/trade")
    print(f"  Consensus expectancy : {avg_c:+.4f} R/trade")
    print(f"  Relative improvement : {rel_improv*100:+.1f}%  (threshold: {IMPROVEMENT_THRESHOLD*100:.0f}%)")

    if rel_improv < IMPROVEMENT_THRESHOLD:
        print(f"\n[!] Improvement below threshold — skipping OpenCode invocation.")
        print(f"    The current scanner.py parameters are already near-optimal.")
        return False, avg_b, avg_c

    print(f"[+] Improvement validated — proceeding with OpenCode.")
    return True, avg_b, avg_c


# ─────────────────────────────────────────────────────────────
# GIT SNAPSHOT
# ─────────────────────────────────────────────────────────────

def git_snapshot() -> bool:
    """
    Commits scanner.py in its current state before OpenCode modifies it.
    Returns True if a snapshot commit was created.
    """
    ts           = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    rel_path     = os.path.join("scanner", "scanner.py")
    commit_msg   = f"pre-optimizer snapshot [{ts}]"

    try:
        # Stage only scanner.py
        subprocess.run(
            ["git", "add", rel_path],
            cwd=REPO_ROOT, check=True, capture_output=True, text=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        if result.returncode == 0:
            sha = result.stdout.strip().split("\n")[0]
            print(f"[+] Git snapshot: {sha}")
            print(f"    Rollback: git checkout HEAD~1 -- {rel_path}")
            return True
        else:
            # "nothing to commit" is not an error
            msg = result.stdout.strip() or result.stderr.strip()
            print(f"[*] Git snapshot skipped: {msg}")
            return False
    except FileNotFoundError:
        print("[!] git not found — skipping snapshot (install git and ensure it's on PATH).")
        return False
    except Exception as e:
        print(f"[!] Git snapshot error: {e} — continuing anyway.")
        return False


# ─────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────

SCANNER_FILE = os.path.join(os.path.dirname(__file__), "scanner.py")

DEFAULTS = ScannerParams()

def build_opencode_prompt(analysis: dict, best_params: dict | None) -> str:
    """Build the structured prompt to send to OpenCode."""

    loss_breakdown = "\n".join(
        f"  - {reason}: {n} losses"
        for reason, n in (analysis.get("exit_reasons") or {}).items()
    )

    tier_breakdown = "\n".join(
        f"  - {tier}: {n} losses"
        for tier, n in (analysis.get("by_tier") or {}).items()
    )

    if best_params:
        param_changes = []
        mapping = {
            "wall_wick_pct":        ("prefetch_all_htf_liquidity → calc_integrity_score",
                                     f"body * {DEFAULTS.wall_wick_pct}", f"body * {best_params.get('wall_wick_pct', DEFAULTS.wall_wick_pct)}"),
            "fuel_wick_pct":        ("prefetch_all_htf_liquidity → fuel wick conditions (6 occurrences)",
                                     f"d_body * {DEFAULTS.fuel_wick_pct}", f"d_body * {best_params.get('fuel_wick_pct', DEFAULTS.fuel_wick_pct)}"),
            "displacement_mult":    ("update_signal_lifecycle → displacement check",
                                     f"avg_body * {DEFAULTS.displacement_mult}", f"avg_body * {best_params.get('displacement_mult', DEFAULTS.displacement_mult)}"),
            "opposite_wick_tol":    ("update_signal_lifecycle → opposite wick check",
                                     f"c_body * {DEFAULTS.opposite_wick_tol}", f"c_body * {best_params.get('opposite_wick_tol', DEFAULTS.opposite_wick_tol)}"),
            "sl_buffer_pct":        ("update_signal_lifecycle → SL calculation",
                                     f"* 1.001 / * 0.999", f"* {1 + best_params.get('sl_buffer_pct', DEFAULTS.sl_buffer_pct):.4f} / * {1 - best_params.get('sl_buffer_pct', DEFAULTS.sl_buffer_pct):.4f}"),
            "proximity_filter_pct": ("update_signal_lifecycle → proximity filter",
                                     f"lv_val * 1.01", f"lv_val * {1 + best_params.get('proximity_filter_pct', DEFAULTS.proximity_filter_pct):.3f}"),
        }
        for k, (location, current, optimal) in mapping.items():
            old_val = getattr(DEFAULTS, k)
            new_val = best_params.get(k, old_val)
            if new_val != old_val:
                param_changes.append(f"  [{location}]\n    FROM: {current}\n    TO:   {optimal}")

        params_section = (
            "PARAMETER CHANGES TO APPLY:\n" + "\n".join(param_changes)
            if param_changes
            else "No parameter changes needed — current values are already optimal."
        )
    else:
        params_section = "Grid search produced insufficient data. No parameter changes recommended."

    prompt = f"""You are a code optimizer for a CRT trading scanner.
File to modify: scanner.py (in the current directory)

CONTEXT:
Automated weekly backtesting has identified suboptimal parameters in scanner.py.
The following analysis is based on {analysis.get('total_losses', 0)} recent LOSS trades.

LOSS BREAKDOWN BY EXIT REASON:
{loss_breakdown or '  No data'}

LOSS BREAKDOWN BY TIER (HTF level):
{tier_breakdown or '  No data'}

AVERAGE SL DISTANCE AT TIME OF LOSS: {analysis.get('avg_sl_distance_pct', 0):.3f}%

{params_section}

INSTRUCTIONS:
1. Read scanner.py carefully.
2. Apply ONLY the parameter changes listed above.
3. Do not change any logic, variable names, comments, or structure beyond the numeric values.
4. After applying changes, confirm with a brief summary of what was changed.

Generated by optimizer_agent.py on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
"""
    return prompt


# ─────────────────────────────────────────────────────────────
# OPENCODE INVOCATION
# ─────────────────────────────────────────────────────────────

def invoke_opencode(prompt: str, dry_run: bool = False):
    """Call OpenCode CLI with the optimization prompt."""
    if dry_run:
        print("\n" + "="*60)
        print("  [DRY RUN] OPENCODE PROMPT (would be sent to OpenCode):")
        print("="*60)
        print(prompt)
        return

    # Write prompt to a temp file to avoid shell escaping issues
    prompt_file = os.path.join(os.path.dirname(__file__), "_optimizer_prompt.txt")
    with open(prompt_file, "w", encoding="utf-8") as f:
        f.write(prompt)

    print(f"\n[*] Invoking OpenCode... (prompt saved to {prompt_file})")
    try:
        result = subprocess.run(
            [OPENCODE_CMD, "run", "--file", prompt_file],
            cwd=os.path.dirname(__file__),
            capture_output=False,
            timeout=300,
        )
        if result.returncode == 0:
            print("\n[+] OpenCode completed successfully.")
        else:
            print(f"\n[!] OpenCode exited with code {result.returncode}.")
    except FileNotFoundError:
        print(f"\n[!] OpenCode not found. Install it first:")
        print(f"      npm install -g opencode-ai")
        print(f"    Then re-run: python optimizer_agent.py")
        print(f"\n    Prompt saved to: {prompt_file}")
    except subprocess.TimeoutExpired:
        print("\n[!] OpenCode timed out after 300s.")
    finally:
        # Clean up prompt file
        if os.path.exists(prompt_file):
            os.remove(prompt_file)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CRT Flow Optimizer Agent")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print analysis and prompt without invoking OpenCode")
    parser.add_argument("--tickers",  nargs="+", default=None,
                        help="Override tickers for grid search (skips Supabase loss pull)")
    parser.add_argument("--no-grid",   action="store_true",
                        help="Skip grid search (only run loss analysis)")
    parser.add_argument("--full-grid", action="store_true",
                        help="Optimize all 6 params (slower, default: only top 3)")
    args = parser.parse_args()

    print("="*60)
    print("  CRT Flow Optimizer Agent")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*60)

    # Shared Supabase client (reused for losses + final log_run)
    supabase = None

    # Accumulate data for the final log row
    run: dict = {
        "dry_run":              args.dry_run,
        "status":               "error",
        "total_losses":         None,
        "top_tickers":          None,
        "exit_reasons":         None,
        "avg_sl_distance_pct":  None,
        "baseline_expectancy":  None,
        "consensus_expectancy": None,
        "improvement_pct":      None,
        "gate_passed":          None,
        "opencode_invoked":     False,
        "params_changed":       None,
        "git_commit_sha":       None,
        "error_message":        None,
    }

    try:
        # ── 1. Pull losses from Supabase ──────────────────────
        analysis = {}
        tickers_for_grid = args.tickers or []

        print("\n[*] Connecting to Supabase...")
        try:
            supabase = setup_supabase()
        except Exception as e:
            print(f"[!] Supabase connection failed: {e}")
            supabase = None

        if not args.tickers and supabase:
            try:
                losses = pull_losses(supabase)
                print(f"[+] Pulled {len(losses)} LOSS trades.")

                if losses:
                    analysis = analyze_loss_patterns(losses)
                    print_analysis(analysis)
                    tickers_for_grid = analysis.get("top_loss_tickers", [])

                    run["total_losses"]        = analysis["total_losses"]
                    run["top_tickers"]         = analysis["top_loss_tickers"]
                    run["exit_reasons"]        = analysis["exit_reasons"]
                    run["avg_sl_distance_pct"] = analysis["avg_sl_distance_pct"]
                else:
                    print("[!] No LOSS trades found in Supabase.")
            except Exception as e:
                print(f"[!] Supabase error: {e}")
                print("    Continuing without loss data...")

        # ── 2. Grid search ────────────────────────────────────
        best_params = None
        data_cache  = {}
        if not args.no_grid and tickers_for_grid:
            mode = "full (6 params)" if args.full_grid else "fast (3 params: wall_wick, fuel_wick, displacement)"
            print(f"\n[*] Running grid search [{mode}] on: {', '.join(tickers_for_grid)}")
            best_params, data_cache = run_grid_search_on_tickers(tickers_for_grid, full_grid=args.full_grid)
            if best_params:
                print(f"\n[+] Consensus best params: {json.dumps(best_params, indent=2)}")
            else:
                print("[!] Grid search returned no results.")
        elif args.no_grid:
            print("\n[*] Skipping grid search (--no-grid).")
        else:
            print("\n[!] No tickers for grid search. Use --tickers or ensure Supabase has LOSS data.")

        # ── 3. Improvement gate ───────────────────────────────
        avg_b, avg_c = 0.0, 0.0
        if best_params and data_cache and not args.dry_run:
            should_proceed, avg_b, avg_c = validate_improvement(best_params, data_cache)
            denom = abs(avg_b) if abs(avg_b) > 0.001 else 0.001
            run["baseline_expectancy"]  = round(avg_b, 5)
            run["consensus_expectancy"] = round(avg_c, 5)
            run["improvement_pct"]      = round((avg_c - avg_b) / denom * 100, 2)
            run["gate_passed"]          = should_proceed

            if not should_proceed:
                run["status"] = "no_improvement"
                print("\n[+] Optimizer Agent finished (no change applied).")
                return

        # ── 4. Build prompt ───────────────────────────────────
        if not analysis and not best_params:
            run["status"] = "no_data"
            print("\n[!] Nothing to optimize. Exiting.")
            return

        prompt = build_opencode_prompt(analysis, best_params)

        # ── 5. Build params_changed diff ─────────────────────
        if best_params:
            defaults = ScannerParams()
            params_changed = {}
            for k in PARAM_GRID:
                old_val = getattr(defaults, k)
                new_val = best_params.get(k, old_val)
                if new_val != old_val:
                    params_changed[k] = {"from": old_val, "to": new_val}
            run["params_changed"] = params_changed or None

        # ── 6. Git snapshot ───────────────────────────────────
        if not args.dry_run and best_params:
            snapshot_created = git_snapshot()
            # Capture the SHA of the snapshot commit
            if snapshot_created:
                try:
                    sha_result = subprocess.run(
                        ["git", "rev-parse", "--short", "HEAD"],
                        cwd=REPO_ROOT, capture_output=True, text=True,
                    )
                    run["git_commit_sha"] = sha_result.stdout.strip() or None
                except Exception:
                    pass

        # ── 7. Invoke OpenCode ────────────────────────────────
        invoke_opencode(prompt, dry_run=args.dry_run)
        run["opencode_invoked"] = not args.dry_run
        run["status"] = "completed"

    except Exception as e:
        run["status"] = "error"
        run["error_message"] = str(e)
        print(f"\n[!] Unexpected error: {e}")
        raise

    finally:
        # ── 8. Log to Supabase (always, even on error) ────────
        if supabase:
            log_run(supabase, run)

    print("\n[+] Optimizer Agent finished.")


if __name__ == "__main__":
    main()
