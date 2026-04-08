"""
build_futures_premium_db.py

Builds (and incrementally updates) a wide-format database of futures roll premiums.

Schema of output CSV (FUTURES_PREMIUM_DB.csv):
  TckrSymb | 20250324_current_px | 20250324_next_current | 20250324_far_next | 20250325_current_px | ...

  - current_px   = M1 (current month) closing price
  - next_current = M2 vs M1 close price % premium
  - far_next     = M3 vs M2 close price % premium

Usage:
  python3 build_futures_premium_db.py [data_dir] [output_csv]

  data_dir   : folder containing BhavCopy_NSE_FO_*.csv files  (default: bhavcopy_data)
  output_csv : path to the database CSV                        (default: FUTURES_PREMIUM_DB.csv)

Incremental behaviour:
  - If output_csv already exists, dates already in it are SKIPPED.
  - Only new files are processed and merged in.
"""

import sys
import os
import glob
import re
import pandas as pd

# ── config ───────────────────────────────────────────────────────────────────
DATA_DIR   = sys.argv[1] if len(sys.argv) > 1 else 'bhavcopy_data'
OUTPUT_CSV = sys.argv[2] if len(sys.argv) > 2 else 'FUTURES_PREMIUM_DB.csv'
# ─────────────────────────────────────────────────────────────────────────────


def extract_date(filename):
    """Extract YYYYMMDD from filename like BhavCopy_NSE_FO_0_0_0_20250324_F_0000.csv"""
    m = re.search(r'(\d{8})', os.path.basename(filename))
    return m.group(1) if m else None


def compute_premiums(filepath):
    """
    Read one BhavCopy CSV and return a DataFrame:
      TckrSymb | current_px | next_current | far_next
    """
    df = pd.read_csv(filepath, low_memory=False)
    stf = df[df['FinInstrmTp'] == 'STF'].copy()
    if stf.empty:
        return pd.DataFrame(columns=['TckrSymb', 'current_px', 'next_current', 'far_next'])

    stf['XpryDt'] = pd.to_datetime(stf['XpryDt'], format='%Y-%m-%d')
    stf = stf.sort_values(['TckrSymb', 'XpryDt'])
    stf['rank'] = stf.groupby('TckrSymb').cumcount() + 1   # 1=M1, 2=M2, 3=M3

    pivot = stf[stf['rank'] <= 3].pivot_table(
        index='TckrSymb',
        columns='rank',
        values='ClsPric',
        aggfunc='first'
    )
    pivot.columns = [f'M{c}' for c in pivot.columns]
    pivot = pivot.reset_index()

    # Replace zero with NaN to avoid division errors
    for col in ['M1', 'M2', 'M3']:
        if col in pivot.columns:
            pivot[col] = pivot[col].replace(0, float('nan'))

    result = pivot[['TckrSymb']].copy()
    result['current_px'] = pivot['M1'] if 'M1' in pivot.columns else float('nan')

    if 'M1' in pivot.columns and 'M2' in pivot.columns:
        result['next_current'] = ((pivot['M2'] - pivot['M1']) / pivot['M1'] * 100).round(2)
    else:
        result['next_current'] = float('nan')

    if 'M2' in pivot.columns and 'M3' in pivot.columns:
        result['far_next'] = ((pivot['M3'] - pivot['M2']) / pivot['M2'] * 100).round(2)
    else:
        result['far_next'] = float('nan')

    return result


# ── load existing database ───────────────────────────────────────────────────
if os.path.exists(OUTPUT_CSV):
    print(f"📂 Loading existing database: {OUTPUT_CSV}")
    db = pd.read_csv(OUTPUT_CSV, index_col='TckrSymb')
    # Dates already processed = column names like 20250324_next_current → extract unique dates
    existing_dates = set()
    for col in db.columns:
        m = re.match(r'^(\d{8})_', col)
        if m:
            existing_dates.add(m.group(1))
    print(f"   {len(existing_dates)} dates already in database")
else:
    db = pd.DataFrame()
    existing_dates = set()
    print(f"🆕 No existing database found — building from scratch")

# ── find all bhavcopy files ──────────────────────────────────────────────────
pattern = os.path.join(DATA_DIR, 'BhavCopy_NSE_FO_*.csv')
all_files = sorted(glob.glob(pattern))
print(f"\n📁 Found {len(all_files)} files in {DATA_DIR}/")

new_files = [(f, extract_date(f)) for f in all_files if extract_date(f) not in existing_dates]
print(f"⚡ {len(new_files)} new files to process\n")

if not new_files:
    print("✅ Database is already up to date.")
    sys.exit(0)

# ── process new files ────────────────────────────────────────────────────────
new_frames = []
for i, (filepath, date) in enumerate(new_files, 1):
    print(f"  [{i:3d}/{len(new_files)}] {date} — {os.path.basename(filepath)}", end='', flush=True)
    premiums = compute_premiums(filepath)
    if premiums.empty:
        print(" (no STF data, skipped)")
        continue
    premiums = premiums.rename(columns={
        'current_px':   f'{date}_current_px',
        'next_current': f'{date}_next_current',
        'far_next':     f'{date}_far_next'
    }).set_index('TckrSymb')
    new_frames.append(premiums)
    print(f"  ✓  {len(premiums)} stocks")

if not new_frames:
    print("\n⚠️  No new data extracted.")
    sys.exit(0)

# ── merge new data into database ─────────────────────────────────────────────
new_data = pd.concat(new_frames, axis=1)

if db.empty:
    db = new_data
else:
    db = db.join(new_data, how='outer')

# Sort columns: latest date first (descending)
date_cols = sorted(db.columns, key=lambda c: c[:8], reverse=True)
db = db[date_cols]

db.index.name = 'TckrSymb'
db = db.sort_index()

# ── save ─────────────────────────────────────────────────────────────────────
db.to_csv(OUTPUT_CSV)
print(f"\n✅ Database saved → {OUTPUT_CSV}")
print(f"   Stocks : {len(db)}")
print(f"   Dates  : {len(db.columns) // 3}")
print(f"   Columns: {len(db.columns)}  ({len(db.columns)//3} dates × 3 metrics)")
