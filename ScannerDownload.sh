#!/bin/bash
cd "$(dirname "$0")"

python3 ~/TEST/AWS/SCANNER/scanner4.py 50000 \
  ~/TEST/AWS/SCANNER/SECTOR_FILTER.csv \
  ~/TEST/AWS/SCANNER/DATA \
  --output SCANNER_PICKS_ALL.json

git add SCANNER_PICKS_ALL.json
git commit -m "update scanner picks"
git push
