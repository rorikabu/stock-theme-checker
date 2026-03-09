import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import re
import time
import threading
import pickle
import json
import tomllib
import base64
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

st.set_page_config(
    page_title="ろりぃ株テーマチェッカー",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── 米国株テーマ（themes_us.json から読み込み） ───────────────────────────────
_US_THEMES_JSON = Path("themes_us.json")

_CATEGORY_COLORS = {
    "テクノロジー":   "#a855f7",
    "消費者一般":     "#f97316",
    "資本財":         "#3b82f6",
    "素材":           "#10b981",
    "エネルギー":     "#f59e0b",
    "金融":           "#06b6d4",
    "ヘルスケア":     "#ec4899",
    "消費者必需品":   "#84cc16",
    "公益":           "#14b8a6",
    "不動産":         "#f43f5e",
    "通信":           "#8b5cf6",
    "その他":         "#6b7280",
}


def load_us_themes() -> list[dict]:
    try:
        with open(_US_THEMES_JSON, encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        st.error(f"themes_us.json 読み込みエラー: {e}")
        return []
    themes = []
    for name, v in raw.items():
        stocks = [s for s in v.get("stocks", []) if "ticker" in s]
        tickers = [s["ticker"] for s in stocks]
        if not tickers:
            continue
        cat = v.get("category", "その他")
        themes.append({
            "name":         name,
            "category":     cat,
            "parent_theme": v.get("parent_theme", ""),
            "cat_color":    _CATEGORY_COLORS.get(cat, "#6b7280"),
            "tickers":      tickers,
            "names":        {s["ticker"]: s["name"] for s in stocks if "name" in s},
            "weights":      {s["ticker"]: s.get("weight", 2) for s in stocks},
        })
    return themes


US_THEMES = load_us_themes()

# ── 日本株テーマ（shikiho_theme_stocks.tsv から読み込み） ─────────────────────
_JP_THEMES_TSV = Path("shikiho_theme_stocks_filtered.tsv")

_JP_CATEGORY_COLORS = {
    "半導体・ＦＰＤ関連": "#a855f7",
    "ハイテク・新技術":    "#8b5cf6",
    "電子部品":            "#7c3aed",
    "インターネット関連":  "#06b6d4",
    "通信・放送":          "#0ea5e9",
    "コンテンツビジネス":  "#f97316",
    "娯楽":                "#fb923c",
    "消費・生活":          "#84cc16",
    "流通・外食":          "#65a30d",
    "自動車関連":          "#f59e0b",
    "景気敏感":            "#ef4444",
    "資源・エネルギー":    "#d97706",
    "環境":                "#10b981",
    "社会資本":            "#3b82f6",
    "社会事象":            "#38bdf8",
    "制度・政策":          "#fbbf24",
    "ビジネストレンド":    "#22d3ee",
    "グローバル":          "#60a5fa",
    "地域":                "#34d399",
    "金融":                "#06b6d4",
    "バイオ":              "#ec4899",
    "福祉・介護":          "#f472b6",
    "ガバナンス":          "#94a3b8",
    "企業":                "#64748b",
    "他製品・サービス":    "#a3a3a3",
    "ディフェンシブ":      "#14b8a6",
}


def load_jp_themes() -> list[dict]:
    import csv
    from collections import defaultdict
    try:
        _WEIGHT_MAP = {"S": 4, "A": 3, "B": 2, "C": 1}
        cat_map:     dict[str, str]        = {}  # テーマ名 → カテゴリ
        stocks_map:  dict[str, list[tuple]] = defaultdict(list)  # テーマ名 → [(code, name, weight)]
        with open(_JP_THEMES_TSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                name = row["テーマ名"]
                cat_map[name] = row["カテゴリ"]
                w = _WEIGHT_MAP.get(row.get("寄与度", "").strip(), 2)
                stocks_map[name].append((row["銘柄コード"], row["銘柄名"], w))
    except Exception as e:
        st.error(f"{_JP_THEMES_TSV} 読み込みエラー: {e}")
        return []
    themes = []
    for name, stocks in stocks_map.items():
        cat = cat_map[name]
        tickers = [s[0] for s in stocks]
        themes.append({
            "name":     name,
            "category": cat,
            "cat_color": _JP_CATEGORY_COLORS.get(cat, "#6b7280"),
            "tickers":  tickers,
            "names":    {s[0]: s[1] for s in stocks},
            "weights":  {s[0]: s[2] for s in stocks},
        })
    return themes


JP_THEMES = load_jp_themes()


def reload_jp_themes():
    """TSV再読み込みでJP_THEMESとall_jp_codesを更新"""
    global JP_THEMES, all_jp_codes
    JP_THEMES = load_jp_themes()
    all_jp_codes = tuple(dict.fromkeys(c for th in JP_THEMES for c in th["tickers"]))

# weight → (ラベル, 色)
_WEIGHT_BADGE = {
    4: ("S", "#d97706"),
    3: ("A", "#FF4444"),
    2: ("B", "#4488FF"),
    1: ("C", "#888888"),
}
# weight → 加重平均用の倍率（B=1.0 を基準）
_WEIGHT_MULTIPLIER = {4: 4.0, 3: 2.0, 2: 1.0, 1: 0.5}
# 銘柄数が少ないテーマの補正強度（大きいほど0%に引き戻す力が強い）
_SHRINKAGE_M = 5

JP_PERIODS = {"Now": "rt", "1D": 2, "5D": 6, "1M": 22}
PERIODS     = {"1D": 2, "5D": 6, "1M": 22}  # 米国株用
JQUANTS_API_KEY = st.secrets["jquants"]["api_key"]


def is_trading_hours() -> bool:
    """東証取引時間中か（平日 9:00〜15:30 JST）"""
    from datetime import time as _t
    now = datetime.now(timezone(timedelta(hours=9)))
    if now.weekday() >= 5:           # 土日
        return False
    return _t(9, 0) <= now.time() <= _t(15, 30)

# ── Tachibana API 定数 ────────────────────────────────────────────────────────
_TACHIBANA_SECRETS = Path(".streamlit/secrets.toml")
_TACHIBANA_COLUMNS = "pDPP,pPRP,pDYRP,pDYWP"  # 現値,前日終値,前日比額,前日比%
_TACHIBANA_AUTH_URL = "https://kabuka.e-shiten.jp/e_api_v4r8/auth/"
_JST = timezone(timedelta(hours=9))
_tachibana_p_no_login = 900


@st.cache_resource
def _tachibana_state():
    """立花証券の接続状態をプロセスメモリで保持（クラウド対応）"""
    return {"price_url": "", "status": "disconnected"}


@st.cache_resource
def _tachibana_fetch_state():
    """立花証券のリアルタイム株価キャッシュ（バックグラウンド取得用）"""
    return {"prices": None, "fetching": False, "ts": 0.0, "fetch_start": 0.0}


@st.cache_resource
def _tachibana_login_guard():
    """再ログイン回数の制限状態（5分間に2回まで）"""
    return {"attempts": [], "locked_until": 0.0}


def _can_attempt_login() -> bool:
    """ログイン試行が許可されているか判定（5分間に2回まで）"""
    guard = _tachibana_login_guard()
    now = datetime.now().timestamp()
    if now < guard["locked_until"]:
        return False
    # 5分以上前の試行を除去
    guard["attempts"] = [t for t in guard["attempts"] if now - t < 300]
    if len(guard["attempts"]) >= 2:
        # 上限到達 → 5分間ロック
        guard["locked_until"] = now + 300
        return False
    return True


def _record_login_attempt():
    """ログイン試行を記録"""
    guard = _tachibana_login_guard()
    guard["attempts"].append(datetime.now().timestamp())


def _tachibana_login(user_id: str, password: str) -> tuple:
    """立花証券にログイン。戻り値: (status, message, price_url)"""
    global _tachibana_p_no_login
    now = datetime.now(_JST).strftime("%Y.%m.%d-%H:%M:%S.000")
    payload = {
        "p_no": str(_tachibana_p_no_login),
        "p_sd_date": now,
        "sCLMID": "CLMAuthLoginRequest",
        "sUserId": user_id,
        "sPassword": password,
    }
    _tachibana_p_no_login += 1
    url = _TACHIBANA_AUTH_URL + "?" + quote(json.dumps(payload, ensure_ascii=False))
    try:
        r = requests.get(url, timeout=15)
        text = None
        for enc in ("utf-8", "cp932", "shift_jis"):
            try:
                text = r.content.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            return ("error", "応答の読み取りに失敗しました", "")
        d = json.loads(text)
    except Exception as e:
        return ("error", f"接続に失敗しました: {e}", "")

    login_code = d.get("688", "")
    login_msg = d.get("689", "")

    if login_code == "0":
        price_url = d.get("871") or d.get("sUrlPrice") or ""
        return ("ok", "ログイン成功", price_url)
    elif login_code in ("10089", "1"):
        return ("need_auth", f"電話認証が必要です: {login_msg}", "")
    else:
        status_287 = d.get("287", "")
        msg_286 = d.get("286", "")
        return ("error", f"ログイン失敗: status={status_287}, msg={msg_286}", "")


def _try_auto_reconnect():
    """セッション切れ時の自動再ログイン（5分間に2回まで）"""
    if not _can_attempt_login():
        return
    state = _tachibana_state()
    try:
        user_id = st.secrets["tachibana"]["user_id"]
        password = st.secrets["tachibana"]["password"]
    except Exception:
        state["status"] = "disconnected"
        return
    _record_login_attempt()
    status, msg, price_url = _tachibana_login(user_id, password)
    if status == "ok" and price_url:
        state["price_url"] = price_url
        state["status"] = "connected"
    elif status == "need_auth":
        state["status"] = "need_auth"
    else:
        state["status"] = "disconnected"


# ── データ取得（米国株・日次ファイルキャッシュ） ──────────────────────────────
_US_CACHE_FILE = Path(".streamlit") / "cache_us_prices.json"


@st.cache_resource
def _us_state():
    """プロセス内メモリキャッシュ: 当日分を保持"""
    return {"df": None, "date": "", "fetching": False, "progress": ""}


def _us_cache_load() -> "pd.DataFrame | None":
    """JSONファイルから読み込み。当日分でなければ None を返す。"""
    try:
        with open(_US_CACHE_FILE, encoding="utf-8") as f:
            cache = json.load(f)
        if cache.get("date") != datetime.now(_JST).strftime("%Y-%m-%d"):
            return None
        idx = pd.to_datetime(cache["dates"])
        df = pd.DataFrame(cache["prices"], index=idx)
        df.index.name = "Date"
        return df
    except Exception:
        return None


def _us_cache_save(df: pd.DataFrame):
    """DataFrame を当日付きで JSON に保存。"""
    try:
        _US_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        cache = {
            "date": datetime.now(_JST).strftime("%Y-%m-%d"),
            "dates": [d.strftime("%Y-%m-%d") for d in df.index],
            "prices": {
                col: [None if pd.isna(v) else round(float(v), 4) for v in df[col]]
                for col in df.columns
            },
        }
        with open(_US_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, separators=(",", ":"))
    except Exception:
        pass


def _fetch_us_yf(tickers: tuple) -> pd.DataFrame:
    """yfinance で銘柄を分割取得（レート制限回避）。"""
    start = datetime.now(_JST) - timedelta(days=45)
    state = _us_state()
    batch_size = 50
    all_dfs = []
    ticker_list = list(tickers)
    total_batches = (len(ticker_list) + batch_size - 1) // batch_size

    for b in range(total_batches):
        batch = ticker_list[b * batch_size : (b + 1) * batch_size]
        state["progress"] = f"{b * batch_size}/{len(ticker_list)}"
        for attempt in range(3):
            try:
                raw = yf.download(batch, start=start, auto_adjust=True, progress=False, threads=True)
                if raw.empty:
                    break
                close = raw["Close"] if len(batch) > 1 else raw["Close"].to_frame(batch[0])
                if not close.empty:
                    all_dfs.append(close)
                break
            except Exception:
                time.sleep(5)
        if b < total_batches - 1:
            time.sleep(2)

    if all_dfs:
        return pd.concat(all_dfs, axis=1)
    return pd.DataFrame(columns=list(tickers))


def _us_bg_fetch(tickers):
    """米国株データをバックグラウンドで取得"""
    state = _us_state()
    if state["fetching"]:
        return
    state["fetching"] = True
    try:
        df = _fetch_us_yf(tickers)
        if not df.empty:
            _us_cache_save(df)
            state["df"] = df
            state["date"] = datetime.now(_JST).strftime("%Y-%m-%d")
    finally:
        state["fetching"] = False
        state["progress"] = ""


# ── データ取得（日本株・J-Quants V2） ─────────────────────────────────────────
_JP_CACHE_FILE = Path(".streamlit") / "jp_data.pkl"


@st.cache_resource
def _jp_state():
    """プロセス内で保持されるバックグラウンドフェッチ状態"""
    return {"data": None, "volume": None, "fresh_ts": 0.0, "fetching": False, "progress": ""}


def _jp_file_load():
    try:
        with open(_JP_CACHE_FILE, "rb") as f:
            cached = pickle.load(f)
        # 旧形式 {"data":, "ts":} → 新形式 {"price":, "volume":, "ts":} に変換
        if "data" in cached and "price" not in cached:
            cached["price"] = cached.pop("data")
            cached["volume"] = None
        return cached
    except Exception:
        return None


def _jp_file_save(price_df, volume_df):
    try:
        _JP_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_JP_CACHE_FILE, "wb") as f:
            pickle.dump({
                "price": price_df,
                "volume": volume_df,
                "ts": datetime.now().timestamp(),
            }, f)
    except Exception:
        pass


def _jp_do_fetch(codes):
    """J-Quants V2 APIから株価データを一括取得（日付ベース）"""
    headers = {"x-api-key": JQUANTS_API_KEY}
    today = datetime.today().date()
    start_date = today - timedelta(days=45)
    code_set = set(codes)
    state = _jp_state()

    # 営業日リスト（土日除外、祝日はAPI応答が空で自動スキップ）
    business_days = [
        start_date + timedelta(days=i)
        for i in range(46)
        if (start_date + timedelta(days=i)).weekday() < 5
    ]
    total_days = len(business_days)

    daily_price = {}
    daily_volume = {}

    for idx, d in enumerate(business_days):
        date_str = d.strftime("%Y%m%d")
        if idx % 10 == 0:
            state["progress"] = f"{idx}/{total_days}日"

        for attempt in range(4):
            try:
                resp = requests.get(
                    "https://api.jquants.com/v2/equities/bars/daily",
                    params={"date": date_str},
                    headers=headers, timeout=30,
                )
                if resp.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                if resp.status_code != 200:
                    break
                records = resp.json().get("data", [])
                if not records:
                    break

                day_prices = {}
                day_volumes = {}
                for item in records:
                    code_4 = item.get("Code", "")[:4]
                    if code_4 in code_set:
                        adj_c = item.get("AdjC")
                        adj_v = item.get("AdjVo")
                        if adj_c is not None:
                            day_prices[code_4] = float(adj_c)
                        if adj_v is not None:
                            day_volumes[code_4] = float(adj_v)

                if day_prices:
                    daily_price[d.isoformat()] = day_prices
                if day_volumes:
                    daily_volume[d.isoformat()] = day_volumes
                break
            except Exception:
                time.sleep(1)
                continue
        time.sleep(0.3)

    state["progress"] = "データ構築中..."

    if daily_price:
        price_df = pd.DataFrame(daily_price).T
        price_df.index = pd.to_datetime(price_df.index)
        price_df.index.name = "Date"
        price_df = price_df.sort_index().astype(float)
    else:
        price_df = pd.DataFrame(columns=list(codes))

    if daily_volume:
        volume_df = pd.DataFrame(daily_volume).T
        volume_df.index = pd.to_datetime(volume_df.index)
        volume_df.index.name = "Date"
        volume_df = volume_df.sort_index().astype(float)
    else:
        volume_df = pd.DataFrame(columns=list(codes))

    return price_df, volume_df


def _jp_bg_fetch(codes):
    """バックグラウンドでデータを取得し状態を更新"""
    state = _jp_state()
    if state["fetching"]:
        return
    state["fetching"] = True
    try:
        price_df, volume_df = _jp_do_fetch(codes)
        _jp_file_save(price_df, volume_df)
        state["data"] = price_df
        state["volume"] = volume_df
        state["fresh_ts"] = datetime.now().timestamp()
    finally:
        state["fetching"] = False


_JP_REFRESH_HOUR = 15
_JP_REFRESH_MIN  = 45


def _jp_needs_refresh(fresh_ts: float) -> bool:
    """毎日15:45以降に1回だけ更新が必要か判定"""
    from datetime import time as _t
    now = datetime.now(_JST)
    today_due = now.replace(hour=_JP_REFRESH_HOUR, minute=_JP_REFRESH_MIN, second=0, microsecond=0)
    if now.time() >= _t(_JP_REFRESH_HOUR, _JP_REFRESH_MIN):
        # 15:45以降 → 今日の15:45より前のデータなら更新
        return fresh_ts < today_due.timestamp()
    else:
        # 15:45より前 → 昨日の15:45より前のデータなら更新
        yesterday_due = today_due - timedelta(days=1)
        return fresh_ts < yesterday_due.timestamp()


def get_jp_data(codes):
    """
    JPデータを返す。
    - キャッシュ有効（当日15:45以降に取得済み）: 即返却
    - キャッシュ古い＋ファイルあり: 古いデータを即返却、バックグラウンドで更新
    - データなし: 同期フェッチ（初回のみ）
    """
    state = _jp_state()

    # 初回: ファイルキャッシュを読み込み
    if state["data"] is None:
        cached = _jp_file_load()
        if cached:
            state["data"] = cached["price"]
            state["volume"] = cached.get("volume")
            state["fresh_ts"] = cached["ts"]

    if _jp_needs_refresh(state["fresh_ts"]) and not state["fetching"]:
        # バックグラウンドで取得（UIをブロックしない）
        t = threading.Thread(target=_jp_bg_fetch, args=(codes,), daemon=True)
        t.start()

    price = state["data"] if state["data"] is not None else pd.DataFrame(columns=list(codes))
    volume = state["volume"]
    return price, volume


# ── Tachibana API（リアルタイム株価） ─────────────────────────────────────────


def _load_tachibana_price_url() -> str:
    state = _tachibana_state()
    if state["price_url"]:
        return state["price_url"]
    try:
        with open(_TACHIBANA_SECRETS, "rb") as f:
            url = tomllib.load(f).get("tachibana", {}).get("price_url", "")
        if url:
            state["price_url"] = url
            state["status"] = "connected"
        return url
    except Exception:
        return ""


def _reset_tachibana_price_url():
    state = _tachibana_state()
    state["price_url"] = ""
    state["status"] = "expired"
    try:
        text = _TACHIBANA_SECRETS.read_text()
        lines = []
        for line in text.splitlines():
            if line.strip().startswith("price_url"):
                lines.append('price_url = ""')
            else:
                lines.append(line)
        _TACHIBANA_SECRETS.write_text("\n".join(lines) + "\n")
    except Exception:
        pass


_TACHIBANA_BATCH = 100  # 1リクエストあたりの最大銘柄数（URL長 ~500字）


_tachibana_p_no_offset = 0  # status=6 リトライ時の補正値


def _fetch_tachibana_batch(batch: tuple, price_url: str, p_no: int, _retry: bool = False) -> dict:
    """1バッチ分の株価を取得して dict を返す（内部用・キャッシュなし）"""
    global _tachibana_p_no_offset
    effective_p_no = p_no + _tachibana_p_no_offset
    now_str = datetime.now(_JST).strftime("%Y.%m.%d-%H:%M:%S.000")
    payload = {
        "p_no": str(effective_p_no),
        "p_sd_date": now_str,
        "sCLMID": "CLMMfdsGetMarketPrice",
        "sTargetIssueCode": ",".join(batch),
        "sTargetColumn": _TACHIBANA_COLUMNS,
    }
    url = price_url + "?" + quote(json.dumps(payload, ensure_ascii=False))
    try:
        r = requests.get(url, stream=True, timeout=15)
        raw = b""
        for chunk in r.iter_content(chunk_size=None):
            raw += chunk
        text = None
        for enc in ("utf-8", "cp932"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            return {}
        d = json.loads(text)
    except Exception:
        return {}

    status = d.get("287", "")
    if status in ("-2", "-3"):
        _reset_tachibana_price_url()
        raise RuntimeError("session_expired")
    # p_no が前回値以下 → エラーメッセージから必要値を抽出してリトライ（1回のみ）
    if status == "6" and not _retry:
        m = re.search(r"前要求\.p_no:\[(\d+)\]", d.get("286", ""))
        if m:
            required = int(m.group(1)) + 1
            _tachibana_p_no_offset += required - effective_p_no
            return _fetch_tachibana_batch(batch, price_url, p_no, _retry=True)
        return {}
    if status != "0":
        return {}

    result = {}
    for item in d.get("71", []):
        code = item.get("473", "")
        if not code:
            continue
        try:
            result[code] = {
                "price":      float(item.get("115", 0)),
                "prev":       float(item.get("181", 0)),
                "change_amt": float(item.get("120", 0)),
                "change_pct": float(item.get("119", 0)),
            }
        except (ValueError, TypeError):
            pass
    return result


def _do_fetch_tachibana_prices(codes: tuple, price_url: str) -> dict | None:
    """Tachibana API で全銘柄を100件ずつバッチ取得。
    戻り値: {code: {price, prev, change_amt, change_pct}} or None
    """
    if not price_url:
        return None
    all_prices: dict = {}
    base_p_no = int(datetime.now().timestamp())
    try:
        for i in range(0, len(codes), _TACHIBANA_BATCH):
            batch = codes[i : i + _TACHIBANA_BATCH]
            batch_result = _fetch_tachibana_batch(batch, price_url, base_p_no + i)
            all_prices.update(batch_result)
            if i + _TACHIBANA_BATCH < len(codes):
                time.sleep(0.5)
    except RuntimeError:
        # セッション切れ：途中データは不完全なので None を返す（保存しない）
        return None
    return all_prices if all_prices else None


def _tachibana_bg_fetch(codes, price_url):
    """バックグラウンドで立花証券の株価を取得"""
    state = _tachibana_fetch_state()
    if state["fetching"]:
        return
    state["fetching"] = True
    state["fetch_start"] = datetime.now().timestamp()
    try:
        prices = _do_fetch_tachibana_prices(codes, price_url)
        if prices is not None:
            state["prices"] = prices
            state["ts"] = datetime.now().timestamp()
    finally:
        state["fetching"] = False
        state["fetch_start"] = 0.0


def get_tachibana_prices(codes, price_url):
    """立花証券の株価を取得（非ブロッキング）。
    キャッシュが新鮮(5分以内)ならそのまま返す。
    古い/なければバックグラウンドで取得開始し、現在のキャッシュを返す。
    """
    if not price_url:
        return None
    state = _tachibana_fetch_state()
    now = datetime.now().timestamp()
    if state["prices"] and (now - state["ts"]) < 300:
        return state["prices"]
    if not state["fetching"]:
        threading.Thread(
            target=_tachibana_bg_fetch,
            args=(codes, price_url),
            daemon=True,
        ).start()
    return state["prices"]


def clear_tachibana_cache():
    """立花証券のキャッシュをクリア"""
    state = _tachibana_fetch_state()
    state["prices"] = None
    state["ts"] = 0.0


# ── ユーティリティ ────────────────────────────────────────────────────────────
def calc_return(series, days):
    s = series.dropna()
    if len(s) < 2:
        return 0.0
    end = s.iloc[-1]
    start = s.iloc[max(0, len(s) - days)]
    if start == 0:
        return 0.0
    return round((end - start) / start * 100, 2)


def hex_to_rgb(h):
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def fmt_price(v):
    """株価フォーマット: 整数なら3桁区切り、小数なら1桁"""
    return f"{int(v):,}" if v == int(v) else f"{v:,.1f}"


def fmt_change(v):
    """前日比フォーマット: 符号付き、整数なら区切り、小数なら1桁"""
    sign = "+" if v >= 0 else ""
    return f"{sign}{int(v):,}" if v == int(v) else f"{sign}{v:,.1f}"


def compute_theme_data(themes, data, days, tachibana=None, use_mixed=False):
    """
    tachibana: {code: {price, prev, change_amt, change_pct, open_price}} or None
    - tachibana が渡された場合: 騰落率に pDYWP を使用（呼び出し側が制御）
    - use_mixed=True: 前日比×0.5 + 寄り比×0.5 のミックス指標を使用
    - tachibana が None: J-Quants 履歴データのみ使用
    - 現在価格表示: Tachibana > J-Quants フォールバック
    """
    result = []
    for theme in themes:
        valid = [t for t in theme["tickers"] if t in data.columns]
        rets = {}
        change_pcts = {}  # 前日比%（ミックス時のみ）
        open_rets = {}    # 寄り比%（ミックス時のみ）
        weights = theme.get("weights", {})
        _op_prices = _opening_prices_state()["prices"] if use_mixed else {}
        for t in theme["tickers"]:
            if tachibana and t in tachibana:
                td = tachibana[t]
                if td["price"] <= 0:
                    continue  # 未約定・データ欠損 → スキップ
                op_price = _op_prices.get(t, 0)
                if use_mixed and op_price > 0:
                    cp = td["change_pct"]
                    op = round((td["price"] - op_price) / op_price * 100, 2)
                    rets[t] = round(cp * 0.5 + op * 0.5, 2)
                    change_pcts[t] = cp
                    open_rets[t] = op
                else:
                    rets[t] = td["change_pct"]
            elif t in valid:
                cp = calc_return(data[t], days)
                op_price = _op_prices.get(t, 0)
                if use_mixed and op_price > 0:
                    s = data[t].dropna()
                    if len(s) >= 1:
                        current = float(s.iloc[-1])
                        op = round((current - op_price) / op_price * 100, 2)
                        rets[t] = round(cp * 0.5 + op * 0.5, 2)
                        change_pcts[t] = cp
                        open_rets[t] = op
                    else:
                        rets[t] = cp
                else:
                    rets[t] = cp
        if rets and weights:
            w_sum = sum(_WEIGHT_MULTIPLIER.get(weights.get(t, 2), 1.0) for t in rets)
            w_total = sum(rets[t] * _WEIGHT_MULTIPLIER.get(weights.get(t, 2), 1.0) for t in rets)
            raw_avg = w_total / w_sum
        elif rets:
            raw_avg = sum(rets.values()) / len(rets)
        else:
            raw_avg = 0.0
        # 銘柄数補正: 少ないテーマは0%方向に引き戻す
        n = len(rets)
        avg = round((n * raw_avg) / (n + _SHRINKAGE_M), 2) if n > 0 else 0.0
        prices = {}
        for t in theme["tickers"]:
            if tachibana and t in tachibana:
                td = tachibana[t]
                prices[t] = {"price": td["price"], "change": td["change_amt"]}
            elif t in valid:
                s = data[t].dropna()
                if len(s) >= 2:
                    prices[t] = {"price": float(s.iloc[-1]), "change": float(s.iloc[-1] - s.iloc[-2])}
                elif len(s) == 1:
                    prices[t] = {"price": float(s.iloc[-1]), "change": 0.0}
        item = {**theme, "avg": avg, "returns": rets, "prices": prices}
        # ミックス時: 内訳データを追加
        if use_mixed and change_pcts:
            _wm = weights
            if _wm:
                _ws = sum(_WEIGHT_MULTIPLIER.get(_wm.get(t, 2), 1.0) for t in change_pcts)
                _cp_avg = sum(change_pcts[t] * _WEIGHT_MULTIPLIER.get(_wm.get(t, 2), 1.0) for t in change_pcts) / _ws if _ws else 0.0
                _op_avg = sum(open_rets[t] * _WEIGHT_MULTIPLIER.get(_wm.get(t, 2), 1.0) for t in open_rets) / _ws if _ws else 0.0
            else:
                _cp_avg = sum(change_pcts.values()) / len(change_pcts)
                _op_avg = sum(open_rets.values()) / len(open_rets)
            _nc = len(change_pcts)
            item["avg_change_pct"] = round((_nc * _cp_avg) / (_nc + _SHRINKAGE_M), 2) if _nc else 0.0
            item["avg_open_ret"] = round((_nc * _op_avg) / (_nc + _SHRINKAGE_M), 2) if _nc else 0.0
            item["change_pcts"] = change_pcts
            item["open_rets"] = open_rets
        result.append(item)
    result.sort(key=lambda x: x["avg"], reverse=True)
    return result


@st.cache_data(show_spinner=False)
def build_theme_list(theme_data, prefix="tl"):
    rows = ""
    is_mixed = bool(theme_data and "change_pcts" in theme_data[0])
    for i, t in enumerate(theme_data):
        avg = t["avg"]
        r_color = THEME["up"] if avg >= 0 else THEME["down"]
        arrow = "▲" if avg >= 0 else "▼"
        sign = "+" if avg >= 0 else ""
        unit = "pt" if is_mixed else "%"
        cc = t["cat_color"]
        r, g, b = hex_to_rgb(cc)
        tag_style = (
            f"background:rgba({r},{g},{b},0.12);"
            f"color:{cc};"
            f"border:1px solid rgba({r},{g},{b},0.3);"
        )
        # ミックス時: テーマ行に前日比・寄り比の内訳を表示
        comp_html = ""
        if is_mixed:
            acp = t.get("avg_change_pct", 0.0)
            aor = t.get("avg_open_ret", 0.0)
            acp_c = THEME["up"] if acp >= 0 else THEME["down"]
            aor_c = THEME["up"] if aor >= 0 else THEME["down"]
            comp_html = (
                f'<span style="font-size:0.72rem;margin-left:6px;">'
                f'<span style="color:{acp_c}">前{acp:+.1f}%</span>'
                f'<span style="color:{THEME["muted"]};margin:0 2px;">|</span>'
                f'<span style="color:{aor_c}">寄{aor:+.1f}%</span>'
                f'</span>'
            )
        names   = t.get("names", {})
        prices  = t.get("prices", {})
        weights = t.get("weights", {})
        _cp_map = t.get("change_pcts", {})
        _or_map = t.get("open_rets", {})
        stocks_html = ""
        for ticker, sr in sorted(t["returns"].items(), key=lambda x: x[1], reverse=True):
            sc = THEME["up"] if sr >= 0 else THEME["down"]
            sa = "▲" if sr >= 0 else "▼"
            ss = "+" if sr >= 0 else ""
            name_span = (
                f'<span class="tl-sname">{names[ticker]}</span>'
                if ticker in names else ""
            )
            pinfo = prices.get(ticker)
            price_html = ""
            if pinfo:
                price_html = (
                    f'<span class="tl-price">{fmt_price(pinfo["price"])}</span>'
                    f'<span class="tl-change">{fmt_change(pinfo["change"])}</span>'
                )
            badge_html = ""
            if weights:
                w = weights.get(ticker, 2)
                label, color = _WEIGHT_BADGE.get(w, ("B", "#4488FF"))
                badge_html = (
                    f'<span class="tl-contrib" '
                    f'style="color:{color};background:rgba(0,0,0,0);'
                    f'border:1px solid {color};">{label}</span>'
                )
            # ミックス時: 前日比%と寄り比%を個別表示
            if is_mixed and ticker in _cp_map:
                cpv = _cp_map[ticker]
                orv = _or_map[ticker]
                cpvc = THEME["up"] if cpv >= 0 else THEME["down"]
                orvc = THEME["up"] if orv >= 0 else THEME["down"]
                ret_html = (
                    f'<span class="tl-sret" style="color:{cpvc}">前{cpv:+.1f}%</span>'
                    f'<span class="tl-sret" style="color:{orvc}">寄{orv:+.1f}%</span>'
                )
            else:
                ret_html = f'<span class="tl-sret" style="color:{sc}">{sa} {ss}{sr:.2f}%</span>'
            stocks_html += (
                f'<div class="tl-stock">'
                f'<span><span class="tl-ticker">{ticker}</span>{name_span}</span>'
                f'<span class="tl-stock-right">{price_html}{badge_html}'
                f'{ret_html}</span>'
                f'</div>'
            )

        uid = f"{prefix}{i}"
        rows += (
            f'<div>'
            f'  <input type="checkbox" id="{uid}" class="tl-chk">'
            f'  <label for="{uid}" class="tl-row">'
            f'    <div class="tl-left">'
            f'      <div class="tl-badge">{i + 1}</div>'
            f'      <span class="tl-name">{t["name"]}</span>'
            f'      <span class="tl-tag" style="{tag_style}">{t["category"]}</span>'
            f'    </div>'
            f'    <div class="tl-right">'
            f'      <span class="tl-ret" style="color:{r_color}">{arrow} {sign}{avg:.2f}{unit}</span>'
            f'{comp_html}'
            f'      <span class="tl-chevron">&#9660;</span>'
            f'    </div>'
            f'  </label>'
            f'  <div class="tl-panel">{stocks_html}</div>'
            f'</div>'
        )

    return f'<div class="tl-wrap">{rows}</div>'


@st.cache_data(show_spinner=False)
def build_compact_list(theme_data, prefix="cp"):
    """ざら場モード用コンパクト表示（2列、50テーマ一覧）"""
    items = theme_data[:50]
    is_mixed = bool(items and "change_pcts" in items[0])
    half = (len(items) + 1) // 2
    columns = [items[:half], items[half:]]

    def _build_col(col_items, start_rank):
        html = ""
        for i, t in enumerate(col_items):
            rank = start_rank + i
            avg = t["avg"]
            r_color = THEME["up"] if avg >= 0 else THEME["down"]
            arrow = "▲" if avg >= 0 else "▼"
            sign = "+" if avg >= 0 else ""
            unit = "pt" if is_mixed else "%"

            names = t.get("names", {})
            prices = t.get("prices", {})
            weights = t.get("weights", {})
            _cp_map = t.get("change_pcts", {})
            _or_map = t.get("open_rets", {})

            stocks_html = ""
            for ticker, sr in sorted(t["returns"].items(), key=lambda x: x[1], reverse=True):
                sc = THEME["up"] if sr >= 0 else THEME["down"]
                sa = "▲" if sr >= 0 else "▼"
                ss = "+" if sr >= 0 else ""
                name_span = (
                    f'<span class="tl-sname">{names[ticker]}</span>'
                    if ticker in names else ""
                )
                pinfo = prices.get(ticker)
                price_html = ""
                if pinfo:
                    price_html = (
                        f'<span class="tl-price">{fmt_price(pinfo["price"])}</span>'
                        f'<span class="tl-change">{fmt_change(pinfo["change"])}</span>'
                    )
                badge_html = ""
                if weights:
                    w = weights.get(ticker, 2)
                    label, color = _WEIGHT_BADGE.get(w, ("B", "#4488FF"))
                    badge_html = (
                        f'<span class="tl-contrib" '
                        f'style="color:{color};background:rgba(0,0,0,0);'
                        f'border:1px solid {color};">{label}</span>'
                    )
                if is_mixed and ticker in _cp_map:
                    cpv = _cp_map[ticker]
                    orv = _or_map[ticker]
                    cpvc = THEME["up"] if cpv >= 0 else THEME["down"]
                    orvc = THEME["up"] if orv >= 0 else THEME["down"]
                    ret_html = (
                        f'<span class="tl-sret" style="color:{cpvc}">前{cpv:+.1f}%</span>'
                        f'<span class="tl-sret" style="color:{orvc}">寄{orv:+.1f}%</span>'
                    )
                else:
                    ret_html = f'<span class="tl-sret" style="color:{sc}">{sa} {ss}{sr:.2f}%</span>'
                stocks_html += (
                    f'<div class="cp-stock">'
                    f'<span class="cp-stock-left"><span class="tl-ticker">{ticker}</span>{name_span}</span>'
                    f'<span class="cp-stock-right">{price_html}{badge_html}'
                    f'{ret_html}</span>'
                    f'</div>'
                )

            uid = f"{prefix}{rank}"
            html += (
                f'<div>'
                f'<input type="checkbox" id="{uid}" class="cp-chk">'
                f'<label for="{uid}" class="cp-row">'
                f'<span class="cp-rank">{rank}</span>'
                f'<span class="cp-name">{t["name"]}</span>'
                f'<span class="cp-ret" style="color:{r_color}">{arrow}{sign}{avg:.2f}{unit}</span>'
                f'<span class="cp-chevron">&#9660;</span>'
                f'</label>'
                f'<div class="cp-panel">{stocks_html}</div>'
                f'</div>'
            )
        return html

    left_html = _build_col(columns[0], 1)
    right_html = _build_col(columns[1], half + 1)

    return (
        f'<div class="cp-wrap">'
        f'<div class="cp-col">{left_html}</div>'
        f'<div class="cp-col">{right_html}</div>'
        f'</div>'
    )


# ── 出来高急騰検出 ──────────────────────────────────────────────────────────


def compute_surge_data(themes, volume_df, price_df, tachibana=None):
    """各銘柄の直近出来高 / 過去5日平均出来高 を計算してテーマ別に集計。"""
    result = []
    for theme in themes:
        surges = {}
        rets = {}
        prices = {}
        for t in theme["tickers"]:
            # 出来高倍率
            if volume_df is not None and t in volume_df.columns:
                vs = volume_df[t].dropna()
                if len(vs) >= 2:
                    latest_vol = float(vs.iloc[-1])
                    past = vs.iloc[max(0, len(vs) - 6) : len(vs) - 1]
                    avg_vol = float(past.mean()) if len(past) > 0 else 0.0
                    surges[t] = round(latest_vol / avg_vol, 2) if avg_vol > 0 else 0.0
                else:
                    surges[t] = 0.0
            # 騰落率
            if tachibana and t in tachibana and tachibana[t]["price"] > 0:
                rets[t] = tachibana[t]["change_pct"]
            elif t in price_df.columns:
                rets[t] = calc_return(price_df[t], 2)
            # 現在価格
            if tachibana and t in tachibana and tachibana[t]["price"] > 0:
                td = tachibana[t]
                prices[t] = {"price": td["price"], "change": td["change_amt"]}
            elif t in price_df.columns:
                s = price_df[t].dropna()
                if len(s) >= 2:
                    prices[t] = {"price": float(s.iloc[-1]),
                                 "change": float(s.iloc[-1] - s.iloc[-2])}
                elif len(s) == 1:
                    prices[t] = {"price": float(s.iloc[-1]), "change": 0.0}

        weights = theme.get("weights", {})
        n_surge = len(surges)
        raw_surge = sum(surges.values()) / n_surge if n_surge > 0 else 1.0
        # 出来高倍率の基準は1.0（変化なし）なので1.0方向に引き戻す
        avg_surge = round((n_surge * raw_surge + _SHRINKAGE_M * 1.0) / (n_surge + _SHRINKAGE_M), 2) if n_surge > 0 else 0.0
        if rets and weights:
            w_sum = sum(_WEIGHT_MULTIPLIER.get(weights.get(t, 2), 1.0) for t in rets)
            w_total = sum(rets[t] * _WEIGHT_MULTIPLIER.get(weights.get(t, 2), 1.0) for t in rets)
            raw_ret = w_total / w_sum
        elif rets:
            raw_ret = sum(rets.values()) / len(rets)
        else:
            raw_ret = 0.0
        n_ret = len(rets)
        avg_ret = round((n_ret * raw_ret) / (n_ret + _SHRINKAGE_M), 2) if n_ret > 0 else 0.0
        result.append({
            **theme,
            "avg_surge": avg_surge,
            "avg_ret": avg_ret,
            "surges": surges,
            "returns": rets,
            "prices": prices,
        })
    result.sort(key=lambda x: x["avg_surge"], reverse=True)
    return result


@st.cache_data(show_spinner=False)
def build_surge_list(surge_data, prefix="sg"):
    """急騰察知リスト: テーマごとに出来高倍率でランキング表示"""
    rows = ""
    for i, t in enumerate(surge_data):
        avg_surge = t["avg_surge"]
        avg_ret = t.get("avg_ret", 0.0)
        s_color = THEME["surge_high"] if avg_surge >= 2.0 else THEME["surge_mid"] if avg_surge >= 1.5 else THEME["muted"]
        r_color = THEME["up"] if avg_ret >= 0 else THEME["down"]
        r_arrow = "▲" if avg_ret >= 0 else "▼"
        r_sign = "+" if avg_ret >= 0 else ""
        cc = t["cat_color"]
        r, g, b = hex_to_rgb(cc)
        tag_style = (
            f"background:rgba({r},{g},{b},0.12);"
            f"color:{cc};"
            f"border:1px solid rgba({r},{g},{b},0.3);"
        )
        names = t.get("names", {})
        prices_d = t.get("prices", {})
        stocks_html = ""
        for ticker, sr in sorted(t["surges"].items(), key=lambda x: x[1], reverse=True):
            stock_ret = t["returns"].get(ticker, 0.0)
            sc = THEME["surge_high"] if sr >= 2.0 else THEME["surge_mid"] if sr >= 1.5 else THEME["muted"]
            rc = THEME["up"] if stock_ret >= 0 else THEME["down"]
            ra = "▲" if stock_ret >= 0 else "▼"
            rs = "+" if stock_ret >= 0 else ""
            name_span = (
                f'<span class="tl-sname">{names[ticker]}</span>'
                if ticker in names else ""
            )
            pinfo = prices_d.get(ticker)
            price_html = ""
            if pinfo:
                price_html = (
                    f'<span class="tl-price">{fmt_price(pinfo["price"])}</span>'
                    f'<span class="tl-change">{fmt_change(pinfo["change"])}</span>'
                )
            stocks_html += (
                f'<div class="tl-stock">'
                f'<span><span class="tl-ticker">{ticker}</span>{name_span}</span>'
                f'<span class="tl-stock-right">{price_html}'
                f'<span class="tl-sret" style="color:{rc}">{ra} {rs}{stock_ret:.2f}%</span>'
                f'<span class="tl-sret" style="color:{sc};margin-left:6px;">{sr:.1f}x</span>'
                f'</span>'
                f'</div>'
            )

        uid = f"{prefix}{i}"
        rows += (
            f'<div>'
            f'  <input type="checkbox" id="{uid}" class="tl-chk">'
            f'  <label for="{uid}" class="tl-row">'
            f'    <div class="tl-left">'
            f'      <div class="tl-badge">{i + 1}</div>'
            f'      <span class="tl-name">{t["name"]}</span>'
            f'      <span class="tl-tag" style="{tag_style}">{t["category"]}</span>'
            f'    </div>'
            f'    <div class="tl-right">'
            f'      <span class="tl-ret" style="color:{r_color}">{r_arrow} {r_sign}{avg_ret:.2f}%</span>'
            f'      <span class="tl-ret" style="color:{s_color};font-size:1rem;margin-left:4px;">{avg_surge:.1f}x</span>'
            f'      <span class="tl-chevron">&#9660;</span>'
            f'    </div>'
            f'  </label>'
            f'  <div class="tl-panel">{stocks_html}</div>'
            f'</div>'
        )

    return f'<div class="tl-wrap">{rows}</div>'


# ── 急変動（モメンタム）検出 ─────────────────────────────────────────────────


_OPENING_PRICES_FILE = Path(".streamlit/opening_prices.json")


@st.cache_resource
def _opening_prices_state():
    """銘柄ごとの寄り付き価格"""
    return {"prices": {}, "_date": "", "_file_loaded": False}


def _load_opening_prices():
    """opening_prices.json から当日の始値を読み込む（GitHub Actions で毎朝9:10に更新）"""
    state = _opening_prices_state()
    today = datetime.now(_JST).strftime("%Y-%m-%d")
    if state["_date"] == today and state["_file_loaded"]:
        return  # 既に読み込み済み
    # 日付変更時はリセット
    if state["_date"] != today:
        state["prices"] = {}
        state["_date"] = today
        state["_file_loaded"] = False
    try:
        with open(_OPENING_PRICES_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("date") == today:
            state["prices"] = {k: float(v) for k, v in data["prices"].items()}
            state["_file_loaded"] = True
    except Exception:
        pass


def _record_opening_prices(tachibana_prices):
    """ファイルにない銘柄を立花証券の初回取得値で個別補完"""
    state = _opening_prices_state()
    today = datetime.now(_JST).strftime("%Y-%m-%d")
    if state["_date"] != today:
        state["prices"] = {}
        state["_date"] = today
        state["_file_loaded"] = False
    for code, data in tachibana_prices.items():
        if code not in state["prices"] and data.get("price", 0) > 0:
            state["prices"][code] = data["price"]


@st.cache_resource
def _momentum_state():
    return {"snapshots": [], "max_snapshots": 12, "last_snapshot_ts": 0.0,
            "opening_scores": None, "opening_ts": 0.0, "_date": ""}


def _compute_theme_scores(themes, tachibana_prices):
    """テーマごとの加重平均スコア（shrinkage補正済み）を {name: float} で返す"""
    scores = {}
    for theme in themes:
        rets = {}
        weights = theme.get("weights", {})
        for t in theme["tickers"]:
            if tachibana_prices and t in tachibana_prices and tachibana_prices[t]["price"] > 0:
                rets[t] = tachibana_prices[t]["change_pct"]
        if rets and weights:
            w_sum = sum(_WEIGHT_MULTIPLIER.get(weights.get(t, 2), 1.0) for t in rets)
            w_total = sum(rets[t] * _WEIGHT_MULTIPLIER.get(weights.get(t, 2), 1.0) for t in rets)
            raw_avg = w_total / w_sum
        elif rets:
            raw_avg = sum(rets.values()) / len(rets)
        else:
            raw_avg = 0.0
        n = len(rets)
        avg = round((n * raw_avg) / (n + _SHRINKAGE_M), 2) if n > 0 else 0.0
        scores[theme["name"]] = avg
    return scores


def record_momentum_snapshot(themes, tachibana_prices):
    """5分間隔でテーマスコアのスナップショットを記録"""
    if not tachibana_prices:
        return
    state = _momentum_state()
    now = time.time()
    today = datetime.now(_JST).strftime("%Y-%m-%d")

    # 日付が変わったらリセット
    if state["_date"] != today:
        state["snapshots"] = []
        state["opening_scores"] = None
        state["opening_ts"] = 0.0
        state["_date"] = today

    # 5分間隔ガード（290秒 = 4分50秒でマージン）
    if now - state["last_snapshot_ts"] < 290:
        return

    scores = _compute_theme_scores(themes, tachibana_prices)
    if not scores:
        return

    state["snapshots"].append({"ts": now, "scores": scores})
    state["last_snapshot_ts"] = now

    # 最大12件保持
    if len(state["snapshots"]) > state["max_snapshots"]:
        state["snapshots"] = state["snapshots"][-state["max_snapshots"]:]

    # 寄り付きスコア（9:00〜9:10の最初の記録）
    if state["opening_scores"] is None:
        from datetime import time as _t
        jst_now = datetime.now(_JST)
        if _t(9, 0) <= jst_now.time() <= _t(9, 10):
            state["opening_scores"] = scores
            state["opening_ts"] = now


def compute_momentum_data(themes, tachibana_prices, lookback_minutes=5):
    """現在スコアと指定分前のスコアの差分を計算"""
    state = _momentum_state()
    current_scores = _compute_theme_scores(themes, tachibana_prices)

    if not current_scores:
        return []

    # lookback_minutes前のスナップショットを探す
    now = time.time()
    target_ts = now - lookback_minutes * 60
    past_scores = None
    for snap in reversed(state["snapshots"]):
        if snap["ts"] <= target_ts:
            past_scores = snap["scores"]
            break

    result = []
    for theme in themes:
        name = theme["name"]
        current = current_scores.get(name, 0.0)
        delta = None
        if past_scores and name in past_scores:
            delta = round(current - past_scores[name], 2)

        # 寄り比
        opening_delta = None
        if state["opening_scores"] and name in state["opening_scores"]:
            opening_delta = round(current - state["opening_scores"][name], 2)

        # 個別銘柄の騰落率と価格
        rets = {}
        prices = {}
        for t in theme["tickers"]:
            if tachibana_prices and t in tachibana_prices:
                rets[t] = tachibana_prices[t]["change_pct"]
                td = tachibana_prices[t]
                prices[t] = {"price": td["price"], "change": td["change_amt"]}

        result.append({
            **theme,
            "avg": current,
            "delta": delta,
            "opening_delta": opening_delta,
            "returns": rets,
            "prices": prices,
        })

    return result


@st.cache_data(show_spinner=False)
def build_momentum_list(momentum_data, prefix="mm"):
    """急変動リスト（通常モード）"""
    rows = ""
    for i, t in enumerate(momentum_data):
        avg = t["avg"]
        delta = t.get("delta")
        r_color = THEME["up"] if avg >= 0 else THEME["down"]
        arrow = "▲" if avg >= 0 else "▼"
        sign = "+" if avg >= 0 else ""
        cc = t["cat_color"]
        r, g, b = hex_to_rgb(cc)
        tag_style = (
            f"background:rgba({r},{g},{b},0.12);"
            f"color:{cc};"
            f"border:1px solid rgba({r},{g},{b},0.3);"
        )
        names = t.get("names", {})
        prices = t.get("prices", {})
        weights = t.get("weights", {})
        stocks_html = ""
        for ticker, sr in sorted(t["returns"].items(), key=lambda x: x[1], reverse=True):
            sc = THEME["up"] if sr >= 0 else THEME["down"]
            sa = "▲" if sr >= 0 else "▼"
            ss = "+" if sr >= 0 else ""
            name_span = (
                f'<span class="tl-sname">{names[ticker]}</span>'
                if ticker in names else ""
            )
            pinfo = prices.get(ticker)
            price_html = ""
            if pinfo:
                price_html = (
                    f'<span class="tl-price">{fmt_price(pinfo["price"])}</span>'
                    f'<span class="tl-change">{fmt_change(pinfo["change"])}</span>'
                )
            badge_html = ""
            if weights:
                w = weights.get(ticker, 2)
                label, color = _WEIGHT_BADGE.get(w, ("B", "#4488FF"))
                badge_html = (
                    f'<span class="tl-contrib" '
                    f'style="color:{color};background:rgba(0,0,0,0);'
                    f'border:1px solid {color};">{label}</span>'
                )
            stocks_html += (
                f'<div class="tl-stock">'
                f'<span><span class="tl-ticker">{ticker}</span>{name_span}</span>'
                f'<span class="tl-stock-right">{price_html}{badge_html}'
                f'<span class="tl-sret" style="color:{sc}">{sa} {ss}{sr:.2f}%</span></span>'
                f'</div>'
            )

        # delta表示
        delta_html = ""
        if delta is not None:
            d_color = THEME["up"] if delta >= 0 else THEME["down"]
            d_arrow = "▲" if delta >= 0 else "▼"
            d_sign = "+" if delta >= 0 else ""
            delta_html = (
                f'<span class="tl-ret" style="color:{d_color};font-size:0.85rem;margin-left:4px;">'
                f'{d_arrow} {d_sign}{delta:.2f}pp</span>'
            )

        uid = f"{prefix}{i}"
        rows += (
            f'<div>'
            f'  <input type="checkbox" id="{uid}" class="tl-chk">'
            f'  <label for="{uid}" class="tl-row">'
            f'    <div class="tl-left">'
            f'      <div class="tl-badge">{i + 1}</div>'
            f'      <span class="tl-name">{t["name"]}</span>'
            f'      <span class="tl-tag" style="{tag_style}">{t["category"]}</span>'
            f'    </div>'
            f'    <div class="tl-right">'
            f'      <span class="tl-ret" style="color:{r_color}">{arrow} {sign}{avg:.2f}%</span>'
            f'{delta_html}'
            f'      <span class="tl-chevron">&#9660;</span>'
            f'    </div>'
            f'  </label>'
            f'  <div class="tl-panel">{stocks_html}</div>'
            f'</div>'
        )

    return f'<div class="tl-wrap">{rows}</div>'


@st.cache_data(show_spinner=False)
def build_momentum_compact(momentum_data, prefix="cmm"):
    """急変動リスト（ざら場モード）"""
    items = momentum_data[:50]
    half = (len(items) + 1) // 2
    columns = [items[:half], items[half:]]

    def _build_col(col_items, start_rank):
        html = ""
        for i, t in enumerate(col_items):
            rank = start_rank + i
            avg = t["avg"]
            delta = t.get("delta")
            r_color = THEME["up"] if avg >= 0 else THEME["down"]
            arrow = "▲" if avg >= 0 else "▼"
            sign = "+" if avg >= 0 else ""

            names = t.get("names", {})
            prices = t.get("prices", {})
            weights = t.get("weights", {})

            stocks_html = ""
            for ticker, sr in sorted(t["returns"].items(), key=lambda x: x[1], reverse=True):
                sc = THEME["up"] if sr >= 0 else THEME["down"]
                sa = "▲" if sr >= 0 else "▼"
                ss = "+" if sr >= 0 else ""
                name_span = (
                    f'<span class="tl-sname">{names[ticker]}</span>'
                    if ticker in names else ""
                )
                pinfo = prices.get(ticker)
                price_html = ""
                if pinfo:
                    price_html = (
                        f'<span class="tl-price">{fmt_price(pinfo["price"])}</span>'
                        f'<span class="tl-change">{fmt_change(pinfo["change"])}</span>'
                    )
                badge_html = ""
                if weights:
                    w = weights.get(ticker, 2)
                    label, color = _WEIGHT_BADGE.get(w, ("B", "#4488FF"))
                    badge_html = (
                        f'<span class="tl-contrib" '
                        f'style="color:{color};background:rgba(0,0,0,0);'
                        f'border:1px solid {color};">{label}</span>'
                    )
                stocks_html += (
                    f'<div class="cp-stock">'
                    f'<span class="cp-stock-left"><span class="tl-ticker">{ticker}</span>{name_span}</span>'
                    f'<span class="cp-stock-right">{price_html}{badge_html}'
                    f'<span class="tl-sret" style="color:{sc}">{sa} {ss}{sr:.2f}%</span></span>'
                    f'</div>'
                )

            # delta表示（コンパクト）
            delta_str = ""
            if delta is not None:
                d_color = THEME["up"] if delta >= 0 else THEME["down"]
                d_sign = "+" if delta >= 0 else ""
                delta_str = (
                    f'<span class="cp-ret" style="color:{d_color};font-size:0.65rem;margin-left:2px;">'
                    f'{d_sign}{delta:.2f}pp</span>'
                )

            uid = f"{prefix}{rank}"
            html += (
                f'<div>'
                f'<input type="checkbox" id="{uid}" class="cp-chk">'
                f'<label for="{uid}" class="cp-row">'
                f'<span class="cp-rank">{rank}</span>'
                f'<span class="cp-name">{t["name"]}</span>'
                f'<span class="cp-ret" style="color:{r_color}">{arrow}{sign}{avg:.2f}%</span>'
                f'{delta_str}'
                f'<span class="cp-chevron">&#9660;</span>'
                f'</label>'
                f'<div class="cp-panel">{stocks_html}</div>'
                f'</div>'
            )
        return html

    left_html = _build_col(columns[0], 1)
    right_html = _build_col(columns[1], half + 1)

    return (
        f'<div class="cp-wrap">'
        f'<div class="cp-col">{left_html}</div>'
        f'<div class="cp-col">{right_html}</div>'
        f'</div>'
    )


# ── データロード ──────────────────────────────────────────────────────────────
all_us_tickers = tuple(dict.fromkeys(t for theme in US_THEMES for t in theme["tickers"]))

# JP: TSVの全銘柄を対象（J-Quantsで取得）
all_jp_codes = tuple(dict.fromkeys(c for th in JP_THEMES for c in th["tickers"]))

# US: 当日キャッシュ優先 → なければバックグラウンドで取得
_us = _us_state()
_today_str = datetime.now(_JST).strftime("%Y-%m-%d")
if _us["df"] is not None and _us["date"] == _today_str:
    us_data = _us["df"]
else:
    _file_df = _us_cache_load()
    if _file_df is not None:
        us_data = _file_df
        _us["df"] = _file_df
        _us["date"] = _today_str
    else:
        us_data = pd.DataFrame(columns=list(all_us_tickers))
        if not _us["fetching"]:
            threading.Thread(target=_us_bg_fetch, args=(all_us_tickers,), daemon=True).start()

# JP: ファイルキャッシュがあれば即返却、なければ同期フェッチ（初回のみ）
jp_data, jp_volume = get_jp_data(all_jp_codes)

# 立花証券: 認証情報があれば自動接続（リブート後も自動で繋がる）
_tachi_has_secrets = False
try:
    _ = st.secrets["tachibana"]["user_id"]
    _tachi_has_secrets = True
except Exception:
    pass
_tachi_st = _tachibana_state()
if _tachi_has_secrets and _tachi_st["status"] in ("expired", "disconnected"):
    _try_auto_reconnect()

# 立花証券リアルタイム株価（1回だけ取得、全タブで共有）
_global_tachi_url = _load_tachibana_price_url()
get_tachibana_prices(all_jp_codes, _global_tachi_url)


# ── ダークモード・コンパクトモード状態 ─────────────────────────────────────────
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False
if "compact_mode" not in st.session_state:
    st.session_state.compact_mode = False

# ── グローバル CSS（外部ファイル読み込み・キャッシュ） ─────────────────────────
@st.cache_resource
def _load_css():
    return Path("style.css").read_text(encoding="utf-8")

st.markdown(f'<style>{_load_css()}</style>', unsafe_allow_html=True)

# ダークモード時: CSS変数を上書き注入
if st.session_state.dark_mode:
    st.markdown("""<style>
:root {
    --bg: #1e2328 !important;
    --text: #e2e8f0 !important;
    --text-sub: #cbd5e0 !important;
    --text-muted: #94a3b8 !important;
    --border: #2d3748 !important;
    --shadow-dark: #161a1e !important;
    --shadow-light: #262c32 !important;
    --accent: #818cf8 !important;
    --accent-hover: #a5b4fc !important;
    --up: #f87171 !important;
    --down: #22d3ee !important;
    --surge-high: #fbbf24 !important;
    --surge-mid: #fb923c !important;
}
[data-baseweb="tab"]:hover { background: rgba(255,255,255,0.04) !important; }
[data-testid="stRadio"] label:hover { background: rgba(255,255,255,0.04) !important; }
.logo-img { filter: brightness(0) invert(1); }
</style>""", unsafe_allow_html=True)

# ── テーマ色辞書（Python 内で使う色を1か所管理） ─────────────────────────────
if st.session_state.dark_mode:
    THEME = {
        "up":         "#f87171",
        "down":       "#22d3ee",
        "muted":      "#94a3b8",
        "text_sub":   "#cbd5e0",
        "surge_high": "#fbbf24",
        "surge_mid":  "#fb923c",
    }
else:
    THEME = {
        "up":         "#ef4444",
        "down":       "#06b6d4",
        "muted":      "#8a94a6",
        "text_sub":   "#4a5568",
        "surge_high": "#f59e0b",
        "surge_mid":  "#fb923c",
    }


# ── ヘッダー ──────────────────────────────────────────────────────────────────
@st.cache_resource
def _load_logo_b64():
    return base64.b64encode(Path("logo.png").read_bytes()).decode()

_logo_b64 = _load_logo_b64()

# ── 立花証券の接続状態（ヘッダー表示用） ──────────────────────────────────
_tachi_st = _tachibana_state()

# ── ヘッダー（HTML横並び + Streamlitウィジェット） ────────────────────────────
# ロゴ + 接続状態を1行で表示
_tachi_dot = ""
if _tachi_has_secrets:
    if _tachi_st["status"] == "connected":
        _tachi_dot = '<span style="color:#22c55e;font-size:0.7rem;margin-left:8px;">● 接続中</span>'
    elif _tachi_st["status"] == "need_auth":
        _tachi_dot = '<span style="color:#f59e0b;font-size:0.7rem;margin-left:8px;">● 要認証</span>'
    else:
        _tachi_dot = '<span style="color:#8a94a6;font-size:0.7rem;margin-left:8px;">○ 未接続</span>'

st.markdown(
    f'<div style="display:flex;align-items:center;">'
    f'<img src="data:image/png;base64,{_logo_b64}" class="logo-img">'
    f'{_tachi_dot}</div>',
    unsafe_allow_html=True,
)

# ボタン行（st.pills で横並び保証）
_compact_icon = "🃏" if st.session_state.compact_mode else "📋"
_dark_icon = "☀️" if st.session_state.dark_mode else "🌙"
_pill_options = [_compact_icon, _dark_icon]
if _tachi_has_secrets:
    _tachi_icon = "🟢" if _tachi_st["status"] == "connected" else "🔌"
    _pill_options.append(_tachi_icon)
_pill_options.append("↺")

_action = st.pills("hdr", _pill_options, default=None,
                    label_visibility="collapsed", key="header_pills")

if _action == _compact_icon:
    st.session_state.compact_mode = not st.session_state.compact_mode
    del st.session_state["header_pills"]
    build_compact_list.clear()
    build_momentum_compact.clear()
    build_momentum_list.clear()
    st.rerun()
elif _action == _dark_icon:
    st.session_state.dark_mode = not st.session_state.dark_mode
    del st.session_state["header_pills"]
    build_theme_list.clear()
    build_surge_list.clear()
    build_compact_list.clear()
    build_momentum_list.clear()
    build_momentum_compact.clear()
    st.rerun()
elif _tachi_has_secrets and _action == _tachi_icon:
    del st.session_state["header_pills"]
    if not _can_attempt_login():
        st.toast("⏳ ログイン試行の上限に達しました（5分後に再試行できます）")
    else:
        uid = st.secrets["tachibana"]["user_id"]
        pwd = st.secrets["tachibana"]["password"]
        _record_login_attempt()
        status, msg, price_url = _tachibana_login(uid, pwd)
        if status == "ok" and price_url:
            _tachi_st["price_url"] = price_url
            _tachi_st["status"] = "connected"
            clear_tachibana_cache()
            st.rerun()
        elif status == "need_auth":
            _tachi_st["status"] = "need_auth"
            st.toast("📞 電話認証後にもう一度押してください")
        else:
            _tachi_st["status"] = "disconnected"
            st.toast(f"エラー: {msg}")
elif _action == "↺":
    del st.session_state["header_pills"]
    reload_jp_themes()
    # 米国株データをバックグラウンドで再取得（ファイルキャッシュも削除）
    _us["df"] = None
    _us["date"] = ""
    try:
        _US_CACHE_FILE.unlink(missing_ok=True)
    except Exception:
        pass
    if not _us["fetching"]:
        threading.Thread(target=_us_bg_fetch, args=(all_us_tickers,), daemon=True).start()
    # 立花証券キャッシュもクリア
    clear_tachibana_cache()
    build_theme_list.clear()
    build_surge_list.clear()
    build_compact_list.clear()
    build_momentum_list.clear()
    build_momentum_compact.clear()
    st.rerun()
st.markdown('<div class="header-line"></div>', unsafe_allow_html=True)

# コンパクトモード時: 画面幅を最大化
if st.session_state.compact_mode:
    st.markdown('<style>.block-container { max-width: 100% !important; }</style>', unsafe_allow_html=True)

# バックグラウンドフェッチ完了時に自動リラン
_state = _jp_state()
if "jp_ts_seen" not in st.session_state:
    st.session_state.jp_ts_seen = _state["fresh_ts"]
elif st.session_state.jp_ts_seen < _state["fresh_ts"]:
    st.session_state.jp_ts_seen = _state["fresh_ts"]
    jp_data = _state["data"]
    jp_volume = _state["volume"]
    st.rerun()


# 定期チェック: バックグラウンドフェッチ完了検知 + リアルタイム更新
@st.fragment(run_every=10)
def _periodic_check():
    _s = _jp_state()
    # 始値読み込み + フォールバック記録 + モメンタムスナップショット（取引時間中）
    _load_opening_prices()
    if is_trading_hours():
        _mm_prices = _tachibana_fetch_state()["prices"]
        if _mm_prices:
            _record_opening_prices(_mm_prices)
            record_momentum_snapshot(JP_THEMES, _mm_prices)
    # J-Quantsバックグラウンドフェッチ完了検知
    if st.session_state.get("jp_ts_seen", 0) < _s["fresh_ts"]:
        st.session_state.jp_ts_seen = _s["fresh_ts"]
        st.rerun(scope="app")
    # 米国株バックグラウンドフェッチ完了検知
    _us_s = _us_state()
    if not _us_s["fetching"] and st.session_state.get("_us_was_fetching"):
        st.session_state._us_was_fetching = False
        if _us_s["df"] is not None:
            st.rerun(scope="app")
    elif _us_s["fetching"]:
        st.session_state._us_was_fetching = True
    # ── 立花証券バックグラウンドフェッチ管理 ──────────────────────────
    _tf = _tachibana_fetch_state()
    _ts = _tachibana_state()
    # タイムアウト: 60秒以上取得中 → 強制リセット（スレッド詰まり対策）
    if _tf["fetching"] and _tf.get("fetch_start", 0) > 0:
        if datetime.now().timestamp() - _tf["fetch_start"] > 60:
            _tf["fetching"] = False
            _tf["fetch_start"] = 0.0
    # 成功検知: tsが更新された → 画面更新
    if st.session_state.get("_tachi_ts_seen", 0) < _tf["ts"]:
        st.session_state._tachi_ts_seen = _tf["ts"]
        st.rerun(scope="app")
    # 失敗検知: fetchingがTrue→Falseになったがtsは未更新
    if not _tf["fetching"] and st.session_state.get("_tachi_was_fetching"):
        st.session_state._tachi_was_fetching = False
        if _tachi_has_secrets and _ts["status"] == "expired":
            _try_auto_reconnect()
            if _ts["status"] == "connected":
                _new_url = _load_tachibana_price_url()
                if _new_url:
                    threading.Thread(target=_tachibana_bg_fetch, args=(all_jp_codes, _new_url), daemon=True).start()
            else:
                st.rerun(scope="app")
        else:
            st.rerun(scope="app")
    elif _tf["fetching"]:
        st.session_state._tachi_was_fetching = True
    # セッション切れ or 未接続（取得中でない場合のみ） → 再接続して再取得
    elif _tachi_has_secrets and _ts["status"] in ("expired", "disconnected"):
        _try_auto_reconnect()
        if _ts["status"] == "connected":
            _tf["ts"] = 0.0
            _new_url = _load_tachibana_price_url()
            if _new_url:
                threading.Thread(target=_tachibana_bg_fetch, args=(all_jp_codes, _new_url), daemon=True).start()
    # リアルタイム株価の自動更新（取引時間中・Now選択時に5分ごと）
    if st.session_state.get("period_jp") == "Now" and is_trading_hours():
        now_ts = datetime.now().timestamp()
        last_rt = st.session_state.get("_last_rt_refresh", 0)
        if now_ts - last_rt > 300 and not _tf["fetching"]:
            st.session_state["_last_rt_refresh"] = now_ts
            # 古いデータを表示したまま、バックグラウンドで最新を取得
            _tf["ts"] = 0.0
            _rt_url = _load_tachibana_price_url()
            if _rt_url:
                threading.Thread(
                    target=_tachibana_bg_fetch,
                    args=(all_jp_codes, _rt_url),
                    daemon=True,
                ).start()

_periodic_check()

# ── タブ別 fragment 関数（タブ内操作でそのタブだけ再描画） ─────────────────────

@st.fragment
def _render_jp_tab():
    _state = _jp_state()
    if _state["fetching"]:
        st.markdown(
            f'<div style="color:{THEME["text_sub"]};font-size:0.72rem;margin-bottom:4px;">● データ更新中... {_state.get("progress", "")}</div>',
            unsafe_allow_html=True,
        )

    _tachibana_prices = _tachibana_fetch_state()["prices"]

    _col_period_jp, _col_order_jp = st.columns([5, 3])
    with _col_period_jp:
        period_jp = st.radio(
            "期間", list(JP_PERIODS.keys()), horizontal=True,
            label_visibility="collapsed", key="period_jp",
        )
    with _col_order_jp:
        order_jp = st.radio(
            "順序", ["▲ ベスト", "▼ ワースト"], horizontal=True,
            label_visibility="collapsed", key="order_jp",
        )

    _is_rt      = (period_jp == "Now")  # リアルタイム
    _trading    = is_trading_hours()
    _use_tachi  = (period_jp in ("Now", "1D")) and _trading and bool(_tachibana_prices)
    days_jp     = 2 if _is_rt else JP_PERIODS[period_jp]
    _tachi_for_compute = _tachibana_prices if _use_tachi else None
    # ミックス指標: 立花接続時 or 始値ファイルあり（Now/1Dのみ）
    _has_opening = bool(_opening_prices_state()["prices"])
    _use_mixed  = (period_jp in ("Now", "1D")) and (_use_tachi or _has_opening)

    if jp_data.empty and not _tachi_for_compute:
        st.markdown(
            f'<p style="color:{THEME["text_sub"]};font-size:0.9rem;margin-top:20px;">'
            'データを取得できませんでした（J-Quants / Tachibana APIエラー）</p>',
            unsafe_allow_html=True,
        )
    else:
        jp_theme_data = compute_theme_data(
            JP_THEMES, jp_data, days_jp,
            tachibana=_tachi_for_compute,
            use_mixed=_use_mixed,
        )
        if order_jp == "▼ ワースト":
            jp_theme_data = list(reversed(jp_theme_data))
        if st.session_state.compact_mode:
            st.markdown(build_compact_list(jp_theme_data[:50], prefix="cpjp"), unsafe_allow_html=True)
        else:
            st.markdown(build_theme_list(jp_theme_data, prefix="jp"), unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    _updated = (
        datetime.fromtimestamp(_state["fresh_ts"]).strftime("%Y-%m-%d %H:%M")
        if _state["fresh_ts"] > 0 else "取得中..."
    )
    if _use_mixed and _use_tachi:
        _price_src = f"ミックス指標：前日比×寄り比（立花証券 {datetime.now(_JST).strftime('%H:%M')}）"
    elif _use_mixed:
        _price_src = "ミックス指標：前日比×寄り比（J-Quants + 始値データ）"
    elif _use_tachi:
        _price_src = f"リアルタイム（立花証券 {datetime.now(_JST).strftime('%H:%M')} 更新）"
    elif _is_rt and _tachibana_fetch_state()["fetching"]:
        _price_src = "リアルタイム取得中..."
    elif _is_rt:
        _price_src = "J-Quants（前日終値・時間外）"
    else:
        _price_src = "J-Quants（前日終値）"
    st.caption(f"履歴: {_updated}（J-Quants 24h）　｜　価格: {_price_src}")


@st.fragment
def _render_surge_tab():
    if jp_volume is None or (isinstance(jp_volume, pd.DataFrame) and jp_volume.empty):
        st.markdown(
            f'<p style="color:{THEME["text_sub"]};font-size:0.9rem;margin-top:20px;">'
            '出来高データがありません。データ更新後に表示されます。</p>',
            unsafe_allow_html=True,
        )
    else:
        _tachibana_prices = _tachibana_fetch_state()["prices"]
        _tachi_for_surge = _tachibana_prices if (
            st.session_state.get("period_jp") == "Now"
            and is_trading_hours()
            and bool(_tachibana_prices)
        ) else None
        surge_data = compute_surge_data(
            JP_THEMES, jp_volume, jp_data,
            tachibana=_tachi_for_surge,
        )
        st.markdown(build_surge_list(surge_data, prefix="sg"), unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    st.caption("出来高倍率 = 直近出来高 / 過去5日平均出来高　｜　J-Quants V2")


@st.fragment
def _render_us_tab():
    _us_s = _us_state()
    if _us_s["fetching"]:
        st.markdown(
            f'<div style="color:{THEME["text_sub"]};font-size:0.72rem;margin-bottom:4px;">● 米国株データ取得中... {_us_s.get("progress", "")}</div>',
            unsafe_allow_html=True,
        )
    _col_period_us, _col_order_us = st.columns([5, 3])
    with _col_period_us:
        period_us = st.radio(
            "期間", list(PERIODS.keys()), horizontal=True,
            label_visibility="collapsed", key="period_us",
        )
    with _col_order_us:
        order_us = st.radio(
            "順序", ["▲ ベスト", "▼ ワースト"], horizontal=True,
            label_visibility="collapsed", key="order_us",
        )
    days_us = PERIODS[period_us]
    us_theme_data = compute_theme_data(US_THEMES, us_data, days_us)
    if order_us == "▼ ワースト":
        us_theme_data = list(reversed(us_theme_data))
    if st.session_state.compact_mode:
        st.markdown(build_compact_list(us_theme_data[:50], prefix="cpus"), unsafe_allow_html=True)
    else:
        st.markdown(build_theme_list(us_theme_data, prefix="us"), unsafe_allow_html=True)
    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    _us_src = "キャッシュ（本日取得済）" if _us["date"] == _today_str else "Yahoo Finance"
    st.caption(
        f"データ日付: {_today_str}　｜　{len(all_us_tickers)}銘柄 / {len(US_THEMES)}テーマ"
        f"　｜　{_us_src}"
    )


@st.fragment
def _render_momentum_tab():
    _tachibana_prices = _tachibana_fetch_state()["prices"]
    _trading = is_trading_hours()

    if not _trading:
        st.markdown(
            f'<p style="color:{THEME["text_sub"]};font-size:0.9rem;margin-top:20px;">'
            '取引時間中（9:00〜15:30）のみ更新されます。</p>',
            unsafe_allow_html=True,
        )
        return

    if not _tachibana_prices:
        st.markdown(
            f'<p style="color:{THEME["text_sub"]};font-size:0.9rem;margin-top:20px;">'
            'リアルタイム株価を取得中です...</p>',
            unsafe_allow_html=True,
        )
        return

    state = _momentum_state()
    n_snapshots = len(state["snapshots"])

    if n_snapshots < 2:
        if n_snapshots == 0:
            msg = "データ蓄積中です（あと約10分）..."
        else:
            msg = "データ蓄積中です（あと約5分）..."
        st.markdown(
            f'<p style="color:{THEME["text_sub"]};font-size:0.9rem;margin-top:20px;">'
            f'{msg}</p>',
            unsafe_allow_html=True,
        )
        return

    _col_sort, _col_lookback = st.columns([5, 3])
    with _col_sort:
        sort_mode = st.radio(
            "ソート", ["変動幅", "▲ 上昇", "▼ 下落"], horizontal=True,
            label_visibility="collapsed", key="momentum_sort",
        )
    with _col_lookback:
        lookback_options = ["5分前", "10分前"]
        if state["opening_scores"]:
            lookback_options.append("寄り比")
        lookback = st.radio(
            "比較", lookback_options, horizontal=True,
            label_visibility="collapsed", key="momentum_lookback",
        )

    if lookback == "10分前":
        lookback_min = 10
    else:
        lookback_min = 5

    momentum_data = compute_momentum_data(JP_THEMES, _tachibana_prices, lookback_minutes=lookback_min)

    # 寄り比の場合、deltaをopening_deltaで上書き
    if lookback == "寄り比":
        for item in momentum_data:
            item["delta"] = item.get("opening_delta")

    # delta=Noneのものを除外
    momentum_data = [d for d in momentum_data if d.get("delta") is not None]

    # ソート
    if sort_mode == "変動幅":
        momentum_data.sort(key=lambda x: abs(x.get("delta") or 0), reverse=True)
    elif sort_mode == "▲ 上昇":
        momentum_data.sort(key=lambda x: (x.get("delta") or 0), reverse=True)
    elif sort_mode == "▼ 下落":
        momentum_data.sort(key=lambda x: (x.get("delta") or 0), reverse=False)

    if not momentum_data:
        st.markdown(
            f'<p style="color:{THEME["text_sub"]};font-size:0.9rem;margin-top:20px;">'
            f'指定期間のデータがまだありません。</p>',
            unsafe_allow_html=True,
        )
        return

    if st.session_state.compact_mode:
        st.markdown(build_momentum_compact(momentum_data[:50], prefix="cmm"), unsafe_allow_html=True)
    else:
        st.markdown(build_momentum_list(momentum_data, prefix="mm"), unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    st.caption(f"スナップショット: {n_snapshots}件（最大12件 / 1時間分）　｜　リアルタイム（立花証券）")


# ── タブ作成 & fragment 呼び出し ──────────────────────────────────────────────
tab_jp, tab_surge, tab_momentum, tab_us = st.tabs([
    "🇯🇵 日本株", "🔥 急騰察知", "⚡ 急変動", "🇺🇸 米国株",
])

with tab_jp:
    _render_jp_tab()
with tab_surge:
    _render_surge_tab()
with tab_momentum:
    _render_momentum_tab()
with tab_us:
    _render_us_tab()
