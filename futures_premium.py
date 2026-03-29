import sys
import pandas as pd

# Accept CSV filename as argument, default to Jan 23 file
filename = sys.argv[1] if len(sys.argv) > 1 else 'BhavCopy_NSE_FO_0_0_0_20260123_F_0000.csv'

print(f"📊 Loading {filename}...")
df = pd.read_csv(filename)

# Filter Stock Futures only
stf = df[df['FinInstrmTp'] == 'STF'].copy()
stf['XpryDt'] = pd.to_datetime(stf['XpryDt'], format='%Y-%m-%d')

# Rank expiries per symbol: 1=current month, 2=next month, 3=next-to-next month
stf = stf.sort_values(['TckrSymb', 'XpryDt'])
stf['expiry_rank'] = stf.groupby('TckrSymb').cumcount() + 1

# Keep only ranks 1-3
stf = stf[stf['expiry_rank'] <= 3]

# Pivot: one row per symbol, columns for each expiry rank
pivot = stf.pivot_table(
    index='TckrSymb',
    columns='expiry_rank',
    values=['ClsPric', 'FinInstrmNm'],
    aggfunc='first'
)

# Flatten multi-level columns
pivot.columns = [f'{col[0]}_M{col[1]}' for col in pivot.columns]
pivot = pivot.reset_index()

# Rename for readability
pivot = pivot.rename(columns={
    'FinInstrmNm_M1': 'M1_Contract',
    'FinInstrmNm_M2': 'M2_Contract',
    'FinInstrmNm_M3': 'M3_Contract',
    'ClsPric_M1': 'M1_Close',
    'ClsPric_M2': 'M2_Close',
    'ClsPric_M3': 'M3_Close',
})

# Replace zero close prices with NaN to avoid division errors
for col in ['M1_Close', 'M2_Close', 'M3_Close']:
    pivot[col] = pivot[col].replace(0, float('nan'))

# Calculate premium percentages
pivot['M2_vs_M1_pct'] = ((pivot['M2_Close'] - pivot['M1_Close']) / pivot['M1_Close'] * 100).round(2)
pivot['M3_vs_M2_pct'] = ((pivot['M3_Close'] - pivot['M2_Close']) / pivot['M2_Close'] * 100).round(2)

# Select display columns
output = pivot[[
    'TckrSymb',
    'M1_Contract', 'M1_Close',
    'M2_Contract', 'M2_Close', 'M2_vs_M1_pct',
    'M3_Contract', 'M3_Close', 'M3_vs_M2_pct'
]]

# Sort by M2 premium descending
output = output.sort_values('M2_vs_M1_pct', ascending=False)

print(f"\n{'='*110}")
print(f"{'FUTURES ROLL PREMIUM ANALYSIS':^110}")
print(f"{'='*110}")
print(output.to_string(index=False))
print(f"\nTotal stocks: {len(output)}")

# Save to CSV
out_file = 'FUTURES_PREMIUM.csv'
output.to_csv(out_file, index=False)
print(f"\n✅ Saved to {out_file}")
