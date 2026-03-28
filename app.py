"""
BtoB営業リスト抽出 SaaS
Streamlit + async Playwright（ProactorEventLoop スレッド分離）
Windows の SelectorEventLoop 制約を回避するため、
スクレイパーは専用スレッドで ProactorEventLoop を作成して実行する。
"""

import asyncio
import queue
import random
import re
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass

import pandas as pd
import streamlit as st

# ─── ブラウザ自動インストール ────────────────────────────────────────────────
# クラウド環境（Streamlit Community Cloud 等）では Chromium が存在しないため、
# アプリ起動時に一度だけ自動インストールする。
# @st.cache_resource によりプロセス再起動まで1回のみ実行される。

@st.cache_resource(show_spinner=False)
def _ensure_chromium() -> bool:
    """Chromium が未インストールの場合のみ playwright install chromium を実行する"""
    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    return result.returncode == 0

_ensure_chromium()

# ─── 設定 ────────────────────────────────────────────────────────────────────

DEFAULT_PASSWORD = "sales2024"
APP_TITLE        = "BtoB営業リスト自動抽出ツール"

SCROLL_TIMES   = 8
PAGE_SLEEP_MIN = 1.5
PAGE_SLEEP_MAX = 3.0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


def get_password() -> str:
    try:
        return st.secrets["APP_PASSWORD"]
    except Exception:
        return DEFAULT_PASSWORD


# ─── データクラス ─────────────────────────────────────────────────────────────

@dataclass
class Company:
    name: str = ""
    website: str = ""
    phone: str = ""


# ─── ユーティリティ ───────────────────────────────────────────────────────────

async def async_sleep(min_s: float = PAGE_SLEEP_MIN, max_s: float = PAGE_SLEEP_MAX) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))


def normalize_keyword(keyword: str) -> str:
    """全角スペース・全角英数を半角に統一し、連続空白を除去する"""
    import unicodedata
    keyword = unicodedata.normalize("NFKC", keyword)   # 全角→半角
    keyword = " ".join(keyword.split())                 # 連続空白を1つに
    return keyword.strip()


def clean_url(url: str) -> str:
    if not url:
        return ""
    match = re.search(r"[?&]q=(https?://[^&]+)", url)
    if match:
        return urllib.parse.unquote(match.group(1))
    if url.startswith("http"):
        return url
    return ""


def format_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return raw
    if re.match(r"^(070|080|090|050)", digits) and len(digits) == 11:
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if re.match(r"^(0120|0800)", digits) and len(digits) == 10:
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    if re.match(r"^0[36]", digits) and len(digits) == 10:
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return raw


# ─── 非同期スクレイピングロジック ─────────────────────────────────────────────

async def collect_place_urls(page, keyword: str, log: list) -> list[tuple[str, str]]:
    from playwright.async_api import TimeoutError as PTimeout

    encoded  = urllib.parse.quote(keyword)
    maps_url = f"https://www.google.com/maps/search/{encoded}/"
    log.append(f"Google Maps を開いています: {maps_url[:60]}...")

    try:
        await page.goto(maps_url, wait_until="commit", timeout=30_000)
    except PTimeout:
        log.append("初期ページ読み込みタイムアウト（続行）")
    except Exception as e:
        log.append(f"goto エラー: {e}")

    await async_sleep(3, 5)

    # ── 単一スポットへのリダイレクト検出 ──────────────────────────────────
    # 例: "IT 大阪" → Google Maps が特定店舗の詳細ページに飛んでしまう場合
    current_url = page.url
    if "/maps/place/" in current_url and "/maps/search/" not in current_url:
        place_title = await page.title()
        log.append(
            f"[!] Google Maps が単一スポット ({place_title}) にリダイレクトしました。"
            f" より具体的なキーワード（例: 'IT企業 大阪'）をお試しください。"
        )
        return []

    try:
        await page.wait_for_selector('div[role="feed"]', timeout=30_000)
        log.append("リストパネル検出")
    except Exception:
        log.append("フィードなし、直接リンクを探します")
        try:
            await page.wait_for_selector('a[href*="/maps/place/"]', timeout=10_000)
            log.append("プレイスリンク検出（直接）")
        except Exception:
            log.append("プレイスリンクが見つかりません")
            return []

    for _ in range(SCROLL_TIMES):
        try:
            await page.evaluate(
                "() => { const f = document.querySelector('div[role=\"feed\"]'); if(f) f.scrollBy(0,2000); }"
            )
            await async_sleep(1.5, 2.5)
        except Exception:
            break

    await async_sleep(2, 3)

    try:
        await page.wait_for_selector('a[href*="/maps/place/"]', timeout=10_000)
    except Exception:
        log.append("スクロール後もリンクなし")
        return []

    locs      = await page.locator('a[href*="/maps/place/"]').all()
    places    = []
    seen_urls = set()

    for loc in locs:
        try:
            name = (await loc.get_attribute("aria-label") or "").strip()
            href = (await loc.get_attribute("href") or "").strip()
            if name and href and href not in seen_urls:
                seen_urls.add(href)
                places.append((name, href))
        except Exception:
            continue

    log.append(f"{len(places)} 件のプレイスを収集")
    return places


async def extract_place_detail(page, name: str, url: str) -> Company:
    from playwright.async_api import TimeoutError as PTimeout
    company = Company(name=name)

    try:
        await page.goto(url, wait_until="commit", timeout=20_000)
        await async_sleep(PAGE_SLEEP_MIN, PAGE_SLEEP_MAX)
    except Exception:
        return company

    for sel in [
        'a[data-item-id="authority"]',
        'a[aria-label*="ウェブサイト"]',
        'a[aria-label*="website" i]',
        'a[data-tooltip*="ウェブサイト"]',
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                href    = await el.get_attribute("href", timeout=4_000)
                cleaned = clean_url(href or "")
                if cleaned:
                    company.website = cleaned
                    break
        except Exception:
            continue

    for sel in ['button[data-item-id^="phone:tel:"]', 'button[aria-label*="電話番号"]']:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                label = await el.get_attribute("aria-label", timeout=3_000) or ""
                m     = re.search(r"[\d\-\+\(\)\s]{7,}", label)
                if m:
                    company.phone = m.group().strip()
                    break
                item_id = await el.get_attribute("data-item-id", timeout=3_000) or ""
                tm = re.search(r"tel:(.+)", item_id)
                if tm:
                    company.phone = format_phone(tm.group(1).strip())
                    break
        except Exception:
            continue

    return company


async def _async_scrape(
    keyword: str,
    result_queue: queue.Queue,
    progress_queue: queue.Queue,
) -> None:
    """非同期スクレイパー本体。ProactorEventLoop 上で実行される。"""
    from playwright.async_api import async_playwright

    log: list[str]     = []
    results: list[Company] = []

    try:
        ua = random.choice(USER_AGENTS)
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                    "--lang=ja",
                    "--ignore-certificate-errors",
                ],
            )
            context = await browser.new_context(
                user_agent=ua,
                locale="ja-JP",
                timezone_id="Asia/Tokyo",
                viewport={"width": 1280, "height": 900},
            )
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            page = await context.new_page()
            progress_queue.put((0.02, "Google Maps を検索中..."))

            places = await collect_place_urls(page, keyword, log)

            if not places:
                await browser.close()
                progress_queue.put((1.0, "完了（結果なし）"))
                result_queue.put(("empty", results, log))
                return

            total = len(places)
            progress_queue.put((0.05, f"{total} 件検出、詳細を取得中..."))

            for idx, (name, url) in enumerate(places):
                pct = 0.05 + 0.95 * ((idx + 1) / total)
                progress_queue.put((pct, f"取得中 {idx+1}/{total}: {name}"))

                company = await extract_place_detail(page, name, url)
                results.append(company)
                log.append(
                    f"[{idx+1}/{total}] {name} | "
                    f"{company.website or '-'} | {company.phone or '-'}"
                )
                await async_sleep(PAGE_SLEEP_MIN, PAGE_SLEEP_MAX)

            await browser.close()

        result_queue.put(("success", results, log))

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log.append(f"例外発生: {e}\n{tb}")
        result_queue.put(("error", f"{e}", log))


def _scraper_worker(
    keyword: str,
    result_queue: queue.Queue,
    progress_queue: queue.Queue,
) -> None:
    """
    バックグラウンドスレッドのエントリポイント。
    Windows の SelectorEventLoop 制約を回避するため、
    ProactorEventLoop を明示的に作成してスクレイパーを実行する。
    """
    if sys.platform == "win32":
        # Windows: ProactorEventLoop がサブプロセス作成に必要
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_async_scrape(keyword, result_queue, progress_queue))
    except Exception as e:
        result_queue.put(("error", str(e), [f"ループレベルエラー: {e}"]))
    finally:
        loop.close()


# ─── Streamlit 側の実行ラッパー ───────────────────────────────────────────────

def run_scraper_threaded(keyword: str, progress_bar, status_placeholder) -> tuple:
    """
    スクレイパーをバックグラウンドスレッドで起動し、
    progress_queue をポーリングして UI を更新する。
    """
    result_q   = queue.Queue()
    progress_q = queue.Queue()

    t = threading.Thread(
        target=_scraper_worker,
        args=(keyword, result_q, progress_q),
        daemon=True,
    )
    t.start()

    while t.is_alive() or not progress_q.empty():
        try:
            pct, msg = progress_q.get_nowait()
            progress_bar.progress(min(float(pct), 1.0), text=msg)
            status_placeholder.info(f"⏳ {msg}")
        except queue.Empty:
            pass
        time.sleep(0.3)

    t.join()
    return result_q.get()


# ─── 認証 ────────────────────────────────────────────────────────────────────

def show_login() -> None:
    st.markdown(
        """
        <div style="text-align:center; padding: 2rem 0 1rem;">
            <h1>📋 BtoB営業リスト抽出ツール</h1>
            <p style="color:gray;">ご利用にはパスワードが必要です</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _, col_c, _ = st.columns([1, 2, 1])
    with col_c:
        with st.form("login_form"):
            pw = st.text_input("パスワード", type="password", placeholder="パスワードを入力")
            ok = st.form_submit_button("ログイン", use_container_width=True)
        if ok:
            if pw == get_password():
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("パスワードが正しくありません。")


# ─── 結果表示 ─────────────────────────────────────────────────────────────────

def _show_results(df: pd.DataFrame, keyword: str) -> None:
    st.success(f"✅ **{len(df)} 件** の企業情報を抽出しました（キーワード: `{keyword}`）")

    has_url   = (df["WebサイトURL"] != "").sum()
    has_phone = (df["電話番号"] != "").sum()
    c1, c2, c3 = st.columns(3)
    c1.metric("取得件数",     f"{len(df)} 社")
    c2.metric("URL あり",     f"{has_url} 社")
    c3.metric("電話番号あり", f"{has_phone} 社")

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={"WebサイトURL": st.column_config.LinkColumn("WebサイトURL")},
    )

    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        label="⬇️ target_list.csv をダウンロード",
        data=csv_bytes,
        file_name="target_list.csv",
        mime="text/csv",
        use_container_width=True,
        type="primary",
    )


# ─── メイン画面 ───────────────────────────────────────────────────────────────

def show_main() -> None:
    col_title, col_logout = st.columns([5, 1])
    with col_title:
        st.title("📋 BtoB営業リスト自動抽出")
    with col_logout:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("ログアウト"):
            st.session_state["authenticated"] = False
            st.rerun()

    st.markdown("Google Maps のキーワード検索から企業名・WebサイトURL・電話番号を自動抽出します。")
    st.divider()

    with st.form("scrape_form"):
        keyword = st.text_input(
            "🔍 検索キーワード",
            placeholder="例：Web制作会社 大阪 ／ IT企業 東京 ／ 人材派遣 名古屋",
            help="Google Maps で検索するキーワードを入力してください。",
        )
        col_btn, col_info = st.columns([2, 3])
        with col_btn:
            submitted = st.form_submit_button(
                "🚀 リスト抽出開始", use_container_width=True, type="primary"
            )
        with col_info:
            st.caption("※ 件数に応じて数分〜10分以上かかります。")

    # 既存結果の保持表示
    if "result_df" in st.session_state and not submitted:
        _show_results(
            st.session_state["result_df"],
            st.session_state.get("result_keyword", ""),
        )

    if not submitted:
        return

    if not keyword.strip():
        st.warning("キーワードを入力してください。")
        return

    if st.session_state.get("scraping"):
        st.warning("現在抽出中です。完了までお待ちください。")
        return

    # ── 実行 ─────────────────────────────────────────────────────────────────
    st.divider()
    st.markdown(f"**キーワード：** `{keyword.strip()}`")

    progress_bar        = st.progress(0.0, text="準備中...")
    status_placeholder  = st.empty()
    debug_expander      = st.expander("実行ログ（詳細）", expanded=False)

    st.session_state["scraping"] = True
    status, payload, log = "error", "不明なエラー", []

    try:
        status, payload, log = run_scraper_threaded(
            normalize_keyword(keyword), progress_bar, status_placeholder
        )
    except Exception as e:
        import traceback
        status  = "error"
        payload = str(e)
        log     = [traceback.format_exc()]
    finally:
        st.session_state["scraping"] = False

    progress_bar.empty()
    status_placeholder.empty()

    with debug_expander:
        for line in log:
            st.text(line)

    if status == "error":
        st.error(f"エラーが発生しました\n\n```\n{payload}\n```")
        return

    if status == "empty" or not payload:
        # ログにリダイレクト検出メッセージがあればより詳しく案内
        redirect_hint = next(
            (l for l in log if "単一スポット" in l or "リダイレクト" in l), None
        )
        if redirect_hint:
            st.warning(
                f"**Google Maps が特定のスポットに直接飛んでしまいました。**\n\n"
                f"> {redirect_hint}\n\n"
                "**解決策:** キーワードをより具体的な業種に変えてください。\n\n"
                "| 代わりに使えるキーワード例 |\n"
                "|---|\n"
                "| `IT企業 大阪` |\n"
                "| `システム開発会社 大阪` |\n"
                "| `ソフトウェア会社 大阪` |"
            )
        else:
            st.warning(
                "検索結果が見つかりませんでした。\n\n"
                "「実行ログ（詳細）」を展開して内容を確認し、キーワードを変えて再試行してください。"
            )
        return

    results: list[Company] = payload
    df = pd.DataFrame(
        [{"企業名": c.name, "WebサイトURL": c.website, "電話番号": c.phone}
         for c in results]
    )
    df = df[df["企業名"].str.strip() != ""].drop_duplicates(subset=["企業名"])

    st.session_state["result_df"]      = df
    st.session_state["result_keyword"] = normalize_keyword(keyword)
    _show_results(df, keyword.strip())


# ─── エントリポイント ─────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📋",
        layout="centered",
        initial_sidebar_state="collapsed",
    )

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
    if "scraping" not in st.session_state:
        st.session_state["scraping"] = False

    if not st.session_state["authenticated"]:
        show_login()
    else:
        show_main()


if __name__ == "__main__":
    main()
