"""
fetch_jquants.py  ── 日付別全銘柄取得版
===========================================
戦略: 1日ずつ全銘柄データを取得してから対象銘柄にフィルタ
  - /v2/equities/bars/daily?date=YYYYMMDD → その日の全銘柄（~4400件）を1リクエストで返す
  - 過去250営業日 × 1リクエスト = 約250リクエスト
  - 0.5秒スリープ → 合計 約2〜3分

使い方: venv/bin/python3 -u fetch_jquants.py
"""
import csv
import os
import pickle
import time
import tomllib
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

# ── 設定 ──────────────────────────────────────────────────────────────────────
SECRETS_FILE  = Path(".streamlit/secrets.toml")
TSV_FILE      = Path("shikiho_theme_stocks_filtered.tsv")
OUTPUT_FILE   = Path(".streamlit/jp_data.pkl")
DAYS_BACK     = int(os.environ.get("DAYS_BACK", 400))
SLEEP_SEC     = 0.5    # リクエスト間隔（秒）
PROGRESS_STEP = 50     # 何日ごとに進捗表示するか

# ── APIキー（環境変数 → secrets.toml の優先順） ───────────────────────────────
API_KEY = os.environ.get("JQUANTS_API_KEY")
if not API_KEY:
    with open(SECRETS_FILE, "rb") as f:
        API_KEY = tomllib.load(f)["jquants"]["api_key"]

HEADERS = {"x-api-key": API_KEY}

# ── 対象銘柄コード（TSVから4桁コードを収集） ──────────────────────────────────
target_codes_4 = set(dict.fromkeys(
    row["銘柄コード"]
    for row in csv.DictReader(
        open(TSV_FILE, encoding="utf-8-sig"), delimiter="\t"
    )
))
print(f"対象銘柄: {len(target_codes_4)} コード（TSVから）")

# ── 営業日リストを生成（土日除く、祝日は取得時に空応答で自動スキップ） ─────────
today = datetime.today().date()
start_date = today - timedelta(days=DAYS_BACK)
business_days = [
    start_date + timedelta(days=i)
    for i in range(DAYS_BACK + 1)
    if (start_date + timedelta(days=i)).weekday() < 5  # 月〜金
]
print(f"候補営業日: {len(business_days)} 日（{business_days[0]} 〜 {business_days[-1]}）")
print(f"推定所要時間: 約 {len(business_days) * SLEEP_SEC / 60:.0f} 分")
print("─" * 60)

# ── 日付別に全銘柄取得 ─────────────────────────────────────────────────────────
# 蓄積形式: {date_str: {code_4: value}}
daily_price:  dict[str, dict[str, float]] = {}
daily_volume: dict[str, dict[str, float]] = {}
trading_days_found = 0
t_start = time.time()

for idx, d in enumerate(business_days):
    date_str = d.strftime("%Y%m%d")

    for attempt in range(4):
        try:
            r = requests.get(
                "https://api.jquants.com/v2/equities/bars/daily",
                params={"date": date_str},
                headers=HEADERS,
                timeout=30,
            )
            if r.status_code == 429:
                wait = 2 ** attempt          # 1, 2, 4, 8 秒
                time.sleep(wait)
                continue
            if r.status_code != 200:
                break                        # 400 など → スキップ
            records = r.json().get("data", [])
            if not records:
                break                        # 祝日・休場日 → スキップ

            # 対象コードのみフィルタ（API は5桁コードを返す → 先頭4桁で照合）
            day_prices:  dict[str, float] = {}
            day_volumes: dict[str, float] = {}
            for item in records:
                code_5 = item.get("Code", "")
                code_4 = code_5[:4]           # "72030" → "7203"
                if code_4 in target_codes_4:
                    adj_c = item.get("AdjC")
                    adj_v = item.get("AdjVo")
                    if adj_c is not None:
                        day_prices[code_4] = float(adj_c)
                    if adj_v is not None:
                        day_volumes[code_4] = float(adj_v)

            if day_prices:
                daily_price[d.isoformat()] = day_prices
                trading_days_found += 1
            if day_volumes:
                daily_volume[d.isoformat()] = day_volumes
            break

        except Exception as e:
            time.sleep(1)
            continue

    # 進捗表示
    done = idx + 1
    if done % PROGRESS_STEP == 0 or done == len(business_days):
        elapsed   = time.time() - t_start
        remaining = elapsed / done * (len(business_days) - done)
        print(
            f"[{done:3d}/{len(business_days)}日]"
            f"  営業日: {trading_days_found}日"
            f"  経過: {elapsed/60:.1f}分"
            f"  残り: {remaining/60:.1f}分"
            f"  完了予定: {(datetime.now() + timedelta(seconds=remaining)).strftime('%H:%M')}",
            flush=True,
        )

    time.sleep(SLEEP_SEC)

# ── DataFrame に変換 ───────────────────────────────────────────────────────────
print("─" * 60)
print("DataFrame 構築中...")

# {date: {code: value}} → pivot
if daily_price:
    df_price = pd.DataFrame(daily_price).T
    df_price.index = pd.to_datetime(df_price.index)
    df_price.index.name = "Date"
    df_price = df_price.sort_index().astype(float)
else:
    df_price = pd.DataFrame()

if daily_volume:
    df_volume = pd.DataFrame(daily_volume).T
    df_volume.index = pd.to_datetime(df_volume.index)
    df_volume.index.name = "Date"
    df_volume = df_volume.sort_index().astype(float)
else:
    df_volume = pd.DataFrame()

# ── 保存 ───────────────────────────────────────────────────────────────────────
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_FILE, "wb") as f:
    pickle.dump({
        "price": df_price,
        "volume": df_volume,
        "ts": datetime.now().timestamp(),
    }, f)

elapsed_total = time.time() - t_start
print(f"保存完了: {OUTPUT_FILE}  ({OUTPUT_FILE.stat().st_size / 1024:.0f} KB)")
print(f"営業日数: {len(df_price)} 日")
print(f"銘柄数:   {len(df_price.columns)} / {len(target_codes_4)} コード")
if not df_price.empty:
    print(f"期間:     {df_price.index[0].date()} 〜 {df_price.index[-1].date()}")
print(f"所要時間: {elapsed_total/60:.1f} 分")
