"""
fetch_us.py
yfinance で米国株テーマの全銘柄を取得し cache_us_prices.json に保存するスクリプト。
app.py の _us_cache_load() が読み込めるフォーマットで保存する。

使い方: venv/bin/python3 -u fetch_us.py
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# ── 設定 ──────────────────────────────────────────────────────────────────────
_JST = timezone(timedelta(hours=9))
THEMES_FILE  = Path("themes_us.json")
OUTPUT_FILE  = Path(".streamlit/cache_us_prices.json")
DAYS_BACK    = 400

# ── 銘柄コード収集（themes_us.json から） ─────────────────────────────────────
with open(THEMES_FILE, encoding="utf-8") as f:
    raw = json.load(f)

tickers = list(dict.fromkeys(
    s["ticker"]
    for v in raw.values()
    for s in v.get("stocks", [])
    if "ticker" in s
))
print(f"対象銘柄: {len(tickers)} 件")
print(f"取得期間: 過去 {DAYS_BACK} 日分")

# ── yfinance で一括取得 ────────────────────────────────────────────────────────
start = datetime.now(_JST) - timedelta(days=DAYS_BACK)
df = pd.DataFrame()

for attempt in range(3):
    try:
        print(f"yfinance ダウンロード中... (試行 {attempt + 1}/3)")
        raw_data = yf.download(
            tickers,
            start=start,
            auto_adjust=True,
            progress=False,
        )
        if raw_data.empty:
            print("  → データ空。リトライ...")
            continue
        close = raw_data["Close"]
        if isinstance(close, pd.Series):
            close = close.to_frame(tickers[0])
        if not close.empty:
            df = close
            print(f"  → 取得成功: {len(df.columns)} 銘柄 × {len(df)} 日")
            break
    except Exception as e:
        print(f"  → エラー: {e}")

if df.empty:
    print("取得失敗。cache_us_prices.json を更新しませんでした。")
    raise SystemExit(1)

# ── cache_us_prices.json に保存（app.py と同じフォーマット） ──────────────────
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
cache = {
    "date":   datetime.now(_JST).strftime("%Y-%m-%d"),
    "dates":  [d.strftime("%Y-%m-%d") for d in df.index],
    "prices": {
        col: [None if pd.isna(v) else round(float(v), 4) for v in df[col]]
        for col in df.columns
    },
}
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(cache, f, separators=(",", ":"))

size_kb = OUTPUT_FILE.stat().st_size / 1024
print(f"保存完了: {OUTPUT_FILE}  ({size_kb:.0f} KB)")
print(f"銘柄数:   {len(df.columns)} / {len(tickers)} 件")
print(f"期間:     {df.index[0].date()} 〜 {df.index[-1].date()} ({len(df)} 日)")
