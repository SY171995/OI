"""
btst_screener.py

BTST (Buy Today Sell Tomorrow) / STBT (Sell Today Buy Tomorrow) screener for NSE F&O stock futures.

Composite score = 0.4 × price_rank + 0.4 × volume_rank + 0.2 × OI_rank
Only M1 (current month) stock futures are considered.

Long build-up (BTST): price up + volume + OI increase
Short build-up (STBT): price down + volume + OI increase  → price rank is inverted

Usage:
  python3 btst_screener.py                  # latest file in bhavcopy_data/
  python3 btst_screener.py 20260409         # specific date
  python3 btst_screener.py --all            # process all files in bhavcopy_data/
  python3 btst_screener.py --top 10         # change N picks (default 5)
  python3 btst_screener.py --build-json     # merge btst_picks/ CSVs → BTST_PICKS_ALL.json

Output: btst_picks/BTST_PICKS_{YYYYMMDD}.csv  (long, per day)
        btst_picks/STBT_PICKS_{YYYYMMDD}.csv  (short, per day)
        BTST_PICKS_ALL.json                    (--build-json, for Vercel)
"""

import os
import sys
import glob
import re
import json
import argparse
import pandas as pd

DATA_DIR  = 'bhavcopy_data'
OUT_DIR   = 'btst_picks'
TOP_N     = 5


def extract_date(filename):
    m = re.search(r'(\d{8})', os.path.basename(filename))
    return m.group(1) if m else None


def get_latest_file():
    files = sorted(glob.glob(os.path.join(DATA_DIR, 'BhavCopy_NSE_FO_*.csv')))
    return files[-1] if files else None


def get_file_for_date(date_str):
    pattern = os.path.join(DATA_DIR, f'BhavCopy_NSE_FO_*{date_str}*.csv')
    files = glob.glob(pattern)
    return files[0] if files else None


def compute_btst(filepath, top_n=TOP_N, side='long'):
    df = pd.read_csv(filepath, low_memory=False)

    # Filter stock futures only
    stf = df[df['FinInstrmTp'] == 'STF'].copy()
    if stf.empty:
        return pd.DataFrame()

    # Assign expiry rank per symbol (M1=1, M2=2, M3=3)
    stf['XpryDt'] = pd.to_datetime(stf['XpryDt'], format='%Y-%m-%d')
    stf = stf.sort_values(['TckrSymb', 'XpryDt'])
    stf['rank'] = stf.groupby('TckrSymb').cumcount() + 1

    # Combined OI change across all expiries (M1+M2+M3) per symbol
    combined_oi = stf.groupby('TckrSymb')['ChngInOpnIntrst'].sum().rename('combined_oi_change')

    # Keep M1 only
    m1 = stf[stf['rank'] == 1].copy()

    # Data quality: need valid prices
    m1 = m1[(m1['PrvsClsgPric'] > 0) & (m1['ClsPric'] > 0)].copy()
    if m1.empty:
        return pd.DataFrame()

    # Merge combined OI back into M1
    m1 = m1.join(combined_oi, on='TckrSymb')

    # --- Raw metrics ---
    m1['price_chg_pct'] = ((m1['ClsPric'] - m1['PrvsClsgPric']) / m1['PrvsClsgPric'] * 100).round(2)
    m1['turnover']      = m1['TtlTrfVal']
    m1['oi_change']     = m1['combined_oi_change']

    # Futures premium over spot
    m1['futures_premium_pct'] = ((m1['ClsPric'] - m1['UndrlygPric']) / m1['UndrlygPric'] * 100).round(2)

    # Combined OI change % (combined change / combined prev OI across all expiries)
    combined_prev_oi = stf.groupby('TckrSymb').apply(
        lambda g: (g['OpnIntrst'] - g['ChngInOpnIntrst']).sum()
    ).rename('combined_prev_oi')
    m1 = m1.join(combined_prev_oi, on='TckrSymb')
    m1['oi_change_pct'] = (m1['combined_oi_change'] / m1['combined_prev_oi'].replace(0, float('nan')) * 100).round(2)

    # For short build-up: only consider stocks where price actually fell
    if side == 'short':
        m1 = m1[m1['price_chg_pct'] < 0].copy()
        if m1.empty:
            return pd.DataFrame()

    # --- Percentile ranks (0–100) ---
    # Long: highest price change = highest rank
    # Short: most negative price change = highest rank (invert)
    if side == 'short':
        m1['price_rank'] = (-m1['price_chg_pct']).rank(pct=True) * 100
    else:
        m1['price_rank'] = m1['price_chg_pct'].rank(pct=True) * 100
    m1['vol_rank']   = m1['turnover'].rank(pct=True) * 100
    m1['oi_rank']    = m1['oi_change'].rank(pct=True) * 100

    # --- Composite score ---
    m1['score'] = (0.4 * m1['price_rank'] + 0.4 * m1['vol_rank'] + 0.2 * m1['oi_rank']).round(1)

    # --- Top N ---
    top = m1.nlargest(top_n, 'score').reset_index(drop=True)
    top.index += 1  # 1-based rank

    # Select output columns
    out = top[['TckrSymb', 'score', 'price_chg_pct', 'ClsPric', 'UndrlygPric',
               'futures_premium_pct', 'TtlTrfVal', 'combined_oi_change',
               'oi_change_pct', 'XpryDt', 'TradDt']].copy()

    out['TtlTrfVal'] = (out['TtlTrfVal'] / 1e7).round(2)   # convert to Crores
    out = out.rename(columns={'TtlTrfVal': 'turnover_cr'})
    out['XpryDt'] = out['XpryDt'].dt.strftime('%d-%b-%Y')

    return out


def print_table(df, date_str, side='long'):
    label = 'BTST PICKS  [LONG BUILD-UP]' if side == 'long' else 'STBT PICKS  [SHORT BUILD-UP]'
    print(f"\n{'='*70}")
    print(f"  {label} — {date_str}  (Top {len(df)})")
    print(f"{'='*70}")
    print(f"  {'#':<3} {'Symbol':<14} {'Score':>6} {'Chg%':>7} {'Close':>8} "
          f"{'Fut Prem%':>10} {'Turnover(Cr)':>13} {'OI Chg%':>8}  Expiry")
    print(f"  {'-'*3} {'-'*14} {'-'*6} {'-'*7} {'-'*8} {'-'*10} {'-'*13} {'-'*8}  {'-'*11}")
    for rank, row in df.iterrows():
        print(f"  {rank:<3} {row['TckrSymb']:<14} {row['score']:>6.1f} "
              f"{row['price_chg_pct']:>+7.2f} {row['ClsPric']:>8.2f} "
              f"{row['futures_premium_pct']:>+10.2f} {row['turnover_cr']:>13.2f} "
              f"{row['oi_change_pct']:>+8.2f}  {row['XpryDt']}")
    print()


def _prefix_for_side(side):
    return 'BTST' if side == 'long' else 'STBT'


def process_file(filepath, top_n, quiet=False):
    date_str = extract_date(filepath)
    if not date_str:
        print(f"Could not extract date from {filepath}")
        return

    os.makedirs(OUT_DIR, exist_ok=True)

    for side in ('long', 'short'):
        out = compute_btst(filepath, top_n, side=side)
        prefix = _prefix_for_side(side)
        csv_path = os.path.join(OUT_DIR, f'{prefix}_PICKS_{date_str}.csv')

        if out.empty:
            if not quiet:
                print(f"[{date_str}] No valid {side} build-up data found.")
            continue

        out.to_csv(csv_path, index_label='rank')

        if not quiet:
            print_table(out, date_str, side=side)
            print(f"Saved → {csv_path}")
        else:
            print(f"[{date_str}] ✓  {prefix} {len(out)} picks → {csv_path}")


def build_json(out_path='BTST_PICKS_ALL.json'):
    """Merge all btst_picks/ CSVs into a single JSON for Vercel static serving."""
    long_files  = sorted(glob.glob(os.path.join(OUT_DIR, 'BTST_PICKS_*.csv')), reverse=True)
    short_files = sorted(glob.glob(os.path.join(OUT_DIR, 'STBT_PICKS_*.csv')), reverse=True)

    if not long_files and not short_files:
        print(f"No files found in {OUT_DIR}/  — run --all first.")
        sys.exit(1)

    dates = []
    picks = {}
    short_picks = {}

    for f in long_files:
        m = re.search(r'BTST_PICKS_(\d{8})\.csv', os.path.basename(f))
        if not m:
            continue
        date = m.group(1)
        if date not in dates:
            dates.append(date)
        picks[date] = json.loads(pd.read_csv(f).to_json(orient='records'))

    for f in short_files:
        m = re.search(r'STBT_PICKS_(\d{8})\.csv', os.path.basename(f))
        if not m:
            continue
        date = m.group(1)
        if date not in dates:
            dates.append(date)
        short_picks[date] = json.loads(pd.read_csv(f).to_json(orient='records'))

    dates.sort(reverse=True)
    payload = {'dates': dates, 'picks': picks, 'short_picks': short_picks}
    with open(out_path, 'w') as fh:
        json.dump(payload, fh, separators=(',', ':'))

    print(f"✅ {out_path} written — {len(dates)} dates ({len(picks)} long, {len(short_picks)} short)")


def main():
    parser = argparse.ArgumentParser(description='BTST screener for NSE F&O futures')
    parser.add_argument('date', nargs='?', help='Date in YYYYMMDD format (default: latest)')
    parser.add_argument('--top', type=int, default=TOP_N, help=f'Number of picks (default: {TOP_N})')
    parser.add_argument('--all', action='store_true', help='Process all files in bhavcopy_data/')
    parser.add_argument('--build-json', action='store_true', help='Merge btst_picks/ CSVs → BTST_PICKS_ALL.json')
    args = parser.parse_args()

    if args.build_json:
        build_json()
        return

    if args.all:
        files = sorted(glob.glob(os.path.join(DATA_DIR, 'BhavCopy_NSE_FO_*.csv')))
        if not files:
            print(f"No files found in {DATA_DIR}/")
            sys.exit(1)
        print(f"Processing {len(files)} files...\n")
        for f in files:
            process_file(f, args.top, quiet=True)
        print(f"\nDone. Picks saved in {OUT_DIR}/")
    elif args.date:
        f = get_file_for_date(args.date)
        if not f:
            print(f"No bhavcopy file found for date {args.date}")
            sys.exit(1)
        process_file(f, args.top)
    else:
        f = get_latest_file()
        if not f:
            print(f"No bhavcopy files found in {DATA_DIR}/")
            sys.exit(1)
        process_file(f, args.top)


if __name__ == '__main__':
    main()
