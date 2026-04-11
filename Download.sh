python3 download_bhavcopy_bulk.py --days 7
python3 build_futures_premium_db.py
python3 btst_screener.py --all
python3 btst_screener.py --build-json

git add FUTURES_PREMIUM_DB.csv BTST_PICKS_ALL.json
git commit -m "update DB + BTST picks"

git push -u origin main
