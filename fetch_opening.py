"""
fetch_opening.py  ── 当日始値取得スクリプト（GitHub Actions用）
================================================================
毎朝 9:10 JST に実行し、yfinance で全対象銘柄の始値を取得。
opening_prices.json に保存する。

使い方: python fetch_opening.py
"""
import csv
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yfinance as yf

# ── 設定 ──────────────────────────────────────────────────────────────────────
TSV_FILE    = Path("shikiho_theme_stocks_filtered.tsv")
OUTPUT_FILE = Path(".streamlit/opening_prices.json")
BATCH_SIZE  = 50
JST         = timezone(timedelta(hours=9))

# ── 対象銘柄コード（TSVから4桁コードを収集） ──────────────────────────────────
target_codes = sorted(set(
    row["銘柄コード"]
    for row in csv.DictReader(
        open(TSV_FILE, encoding="utf-8-sig"), delimiter="\t"
    )
))
print(f"対象銘柄: {len(target_codes)} コード")

# yfinance用ティッカー（.T 付き）
tickers = [f"{code}.T" for code in target_codes]
today_str = datetime.now(JST).strftime("%Y-%m-%d")
print(f"取得日: {today_str}")

# ── yfinance でバッチ取得 ─────────────────────────────────────────────────────
all_opens: dict[str, float] = {}
total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE

for b in range(total_batches):
    batch = tickers[b * BATCH_SIZE : (b + 1) * BATCH_SIZE]
    for attempt in range(3):
        try:
            raw = yf.download(
                batch, period="1d", auto_adjust=True,
                progress=False, threads=True,
            )
            if raw.empty:
                break
            # 日付チェック: 今日のデータか確認
            last_date = raw.index[-1].strftime("%Y-%m-%d")
            if last_date != today_str:
                print(f"  日付不一致: {last_date} != {today_str}（休場日の可能性）")
                break
            # Open列を抽出
            if len(batch) == 1:
                opens = raw["Open"].to_frame(batch[0])
            else:
                opens = raw["Open"]
            for col in opens.columns:
                val = opens[col].iloc[-1]
                if val is not None and val == val:  # NaN check
                    code = col.replace(".T", "")
                    all_opens[code] = round(float(val), 1)
            break
        except Exception as e:
            print(f"  batch {b} attempt {attempt}: {e}")
            time.sleep(3)
    if b < total_batches - 1:
        time.sleep(1)
    if (b + 1) % 10 == 0 or b == total_batches - 1:
        print(f"  [{b + 1}/{total_batches}] {len(all_opens)} 銘柄取得済み")

# ── 保存 ──────────────────────────────────────────────────────────────────────
if all_opens:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {"date": today_str, "prices": all_opens}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"保存完了: {OUTPUT_FILE} ({len(all_opens)} 銘柄, {OUTPUT_FILE.stat().st_size / 1024:.1f} KB)")
else:
    print("始値データなし（休場日の可能性）→ ファイル更新なし")
