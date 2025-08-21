#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPATIC 집회·통제정보 크롤러 (Selenium 목록 기반 mgrSeq 선택 + VWorld 지오코딩)
- 결과 CSV 스키마 (모든 필드는 문자열; 리스트는 JSON 문자열):
  ['년','월','일','start_time','end_time','장소','인원','위도','경도','비고']

요점
- Selenium으로 목록 페이지에서 '행사 및 집회' 포함 게시글만 필터 → 그중 가장 큰 mgrSeq 선택
- 목록 테이블의 날짜 텍스트를 파싱하여 '년/월/일' 채움(YYYY-MM-DD/., 'YYYY년 M월 D일', 범위는 첫 날짜)
- 상세 페이지 파싱은 requests + BeautifulSoup
- 동일 집회 추정: (start_time, end_time) 키로 그룹핑
- 장소/위도/경도는 리스트(JSON 문자열) 저장
- VWorld 지오코딩(후보 확장/스코어링 포함): 장소 리스트와 동일한 순서/길이로 위도·경도 리스트 저장(실패는 null)

의존:
- requests
- beautifulsoup4
- selenium (목록 선택 전용)
"""

import re
import csv
import json
import html
import time
import argparse
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup, NavigableString
from bs4 import FeatureNotFound

# --- Selenium(필수: 목록 선택 전용) -----------------------------------------
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------
# CSV 저장/불러오기
# ---------------------------
def save_csv(records: list[dict], path: str):
    fieldnames = ["년","월","일","start_time","end_time","장소","인원","위도","경도","비고"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

def load_csv(path: str) -> list[dict]:
    if not Path(path).exists():
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)

# ---------------------------
# 중복 판별 & 병합
# ---------------------------
def normalize_place_list(place_str: str) -> str:
    try:
        places = json.loads(place_str)
        places = [p.replace(" ", "").lower() for p in places]
        return "|".join(sorted(places))
    except:
        return place_str

def make_key(r: dict):
    """중복 여부를 판별하기 위한 key"""
    return (
        r["년"], r["월"], r["일"],
        r["start_time"], r["end_time"],
        normalize_place_list(r["장소"])
    )

def merge_records(existing: list[dict], new: list[dict]) -> list[dict]:
    merged = { make_key(r): r for r in existing }
    for r in new:
        key = make_key(r)
        if key not in merged:
            merged[key] = r
        else:
            # 보강: 기존에 비어 있으면 새 데이터로 채움
            for field in ["위도","경도","인원","비고"]:
                if (not merged[key].get(field)) and r.get(field):
                    merged[key][field] = r[field]
    return list(merged.values())

# ---------------------------
# 날짜 추출
# ---------------------------
def extract_ymd_from_title(title: str):
    m = re.search(r"(\d{2})(\d{2})(\d{2})", title)
    if m:
        yy, mm, dd = m.groups()
        return f"20{yy}", mm, dd
    return None


DETAIL_URL_FMT = "https://www.spatic.go.kr/spatic/assem/getInfoView.do?mgrSeq={mgrSeq}"
LIST_URL       = "https://www.spatic.go.kr/spatic/main/assem.do"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# 종로구 관련 키워드
JONGNO_KEYWORDS = [
    "종로구", "광화문", "광화문광장", "경복궁", "안국", "안국역", "인사동",
    "종각", "종로", "세종문화회관", "정부서울청사", "교보빌딩", "정곡빌딩",
    "사직로", "율곡로", "자하문로", "청와대로", "수송동", "신문로", "서린로"
]

# ─────────────── VWorld 설정 ───────────────
# 제공해주신 키를 기본값으로 사용합니다(옵션/환경변수로 덮어쓰기 가능).
DEFAULT_VWORLD_KEY = "46AEEE06-EE1D-3C1F-A4A4-E38D578695E8"
VWORLD_SEARCH_URL = "https://api.vworld.kr/req/search"
VWORLD_ADDR_URL   = "https://api.vworld.kr/req/address"

# ─────────────── 공통 유틸 ───────────────
def normalize_spaces(s: str) -> str:
    return re.sub(r"[ \t\u00A0]+", " ", s).strip()

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = s.replace("\u00ad", "")  # soft hyphen
    s = s.replace("\u00A0", " ") # &nbsp;
    s = s.replace("∼", "~").replace("–", "-").replace("—", "-")
    s = normalize_spaces(s)
    return s

def soup_preprocess(soup: BeautifulSoup):
    # <br> → '\n' 로 바꿔서 줄 단위 파싱 안정화
    for br in soup.find_all("br"):
        br.replace_with(NavigableString("\n"))

# ---------------- 요청(세션/쿠키/헤더) ----------------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(LIST_URL, timeout=15)  # 쿠키 확보
    except requests.RequestException:
        pass
    return s

def fetch_html(url: str, session: Optional[requests.Session] = None) -> str:
    sess = session or build_session()
    headers = HEADERS | {"Referer": LIST_URL}
    r = sess.get(url, headers=headers, timeout=20, allow_redirects=True)
    r.raise_for_status()
    return r.text

# ---------------- 목록 → 게시글 추출 (Selenium 전용) ----------------
def _is_event_post(title: str) -> bool:
    if not title:
        return False
    t = re.sub(r"\s+", " ", title).strip()
    if "행사 및 집회" in t:
        return True
    return ("행사" in t) and ("집회" in t)

def _to_int_or_none(s: str) -> Optional[int]:
    if s is None:
        return None
    m = re.search(r"\d+", str(s))
    return int(m.group(0)) if m else None

def convert_json_to_posts(json_data: List[dict]) -> List[Dict]:
    posts = []
    for item in json_data:
        if not isinstance(item, dict):
            continue
        post = {
            "number": str(item.get("mgrSeq", item.get("id", item.get("seq", "")))),
            "title": item.get("title", item.get("subject", "")),
            "date":  item.get("regDt", item.get("regDate", item.get("date", ""))),
            "views": str(item.get("hitCnt", item.get("viewCnt", item.get("views", "0")))),
            "is_new": item.get("newYn", "N") == "Y",
        }
        if post["number"]:
            posts.append(post)
    return posts

def setup_selenium_driver_for_list(headless: bool=True):
    if not SELENIUM_AVAILABLE:
        return None
    try:
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=" + HEADERS["User-Agent"])
        if headless:
            options.add_argument("--headless=new")
        service = ChromeService()  # Selenium Manager 사용
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        return driver
    except Exception:
        return None

def get_posts_with_selenium_from_list(debug: bool=False) -> List[Dict]:
    driver = setup_selenium_driver_for_list(headless=True)
    if not driver:
        return []
    try:
        driver.get(LIST_URL)
        posts: List[Dict] = []
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.assem_content tr"))
            )
            tbody = driver.find_element(By.CSS_SELECTOR, "tbody.assem_content")
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                tds = row.find_elements(By.TAG_NAME, "td")
                if len(tds) >= 4:
                    number = row.get_attribute("key") or tds[0].text.strip()
                    title  = tds[1].text.strip()
                    date_  = tds[2].text.strip()  # ← 목록 날짜 텍스트
                    views  = tds[3].text.strip()
                    if number:
                        posts.append({
                            "number": number, "title": title, "date": date_, "views": views, "is_new": False
                        })
        except Exception:
            pass

        # 테이블 실패 시 <script> JSON 추출
        if not posts:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            scripts = soup.find_all("script")
            for script in scripts:
                if not script.string:
                    continue
                for pat in [
                    r'var\s+\w*[Ll]ist\w*\s*=\s*(\[.*?\]);',
                    r'\w*[Dd]ata\w*\s*=\s*(\[.*?\]);',
                    r'resultList["\']?\s*:\s*(\[.*?\])',
                    r'(\[{[^}]*["\']mgrSeq["\'][^}]*}[^]]*\])'
                ]:
                    m = re.search(pat, script.string, re.DOTALL)
                    if m:
                        try:
                            data = json.loads(m.group(1))
                            posts = convert_json_to_posts(data)
                            break
                        except Exception:
                            continue
                if posts:
                    break

        if debug:
            print(f"[디버그] Selenium 목록 추출: {len(posts)}개")
        return posts
    finally:
        try:
            driver.quit()
        except Exception:
            pass

def get_list_posts(debug: bool=False) -> List[Dict]:
    return get_posts_with_selenium_from_list(debug=debug)

def filter_event_posts(posts: List[Dict], limit: int = 10) -> List[Dict]:
    head = posts[:limit] if posts else []
    return [p for p in head if _is_event_post(p.get("title", ""))]

def select_highest_post(posts: List[Dict]) -> Optional[Dict]:
    best = None
    best_val = None
    for p in posts:
        iv = _to_int_or_none(p.get("number"))
        if iv is None:
            continue
        if best is None or iv > best_val:
            best = p
            best_val = iv
    return best

# ---------------- 목록 날짜 → 년/월/일 파싱 ----------------
def parse_list_date_to_ymd(date_text: str) -> Tuple[str, str, str]:
    if not date_text:
        return "", "", ""
    s = clean_text(date_text)
    m = re.search(r'(\d{4})\D{0,3}(\d{1,2})\D{0,3}(\d{1,2})', s)
    if not m:
        return "", "", ""
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return y, f"{int(mo):02d}", f"{int(d):02d}"

# ---------------- 상세 파싱 ----------------
TIME_PAT_CELL = re.compile(
    r"(?<!\d)(\d{1,2})\s*:\s*(\d{2})(?:\s*[~∼\-]\s*(\d{1,2})\s*:\s*(\d{2}))?"
)

def best_table_by_content(soup: BeautifulSoup, debug: bool=False) -> Optional[BeautifulSoup]:
    best = None
    best_score = (-1, -1)
    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if len(trs) < 2:
            continue
        time_hits = 0
        long_hits = 0
        sample_rows = trs[1: min(len(trs), 12)]
        for tr in sample_rows:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            row_txts = [clean_text(c.get_text("\n")) for c in cells]
            if any(TIME_PAT_CELL.search(t) for t in row_txts):
                time_hits += 1
            if any(len(t) >= 10 for t in row_txts):
                long_hits += 1
        score = (time_hits, long_hits)
        if score > best_score:
            best_score = score
            best = table
    if debug and best:
        print(f"[디버그] best_table_by_content 점수: time_hits={best_score[0]}, long_hits={best_score[1]}")
    return best

def detect_columns(table: BeautifulSoup, debug: bool=False) -> Tuple[int,int,Optional[int],int]:
    trs = table.find_all("tr")
    header_rows_used = 1
    for hdr_rows in [1, 2]:
        if len(trs) < hdr_rows + 1:
            break
        data_cells = trs[hdr_rows].find_all(["td", "th"])
        ncols = len(data_cells) if data_cells else 0
        if ncols == 0:
            continue
        col_texts = [[] for _ in range(ncols)]
        sample_rows = trs[hdr_rows: min(len(trs), hdr_rows + 15)]
        for tr in sample_rows:
            cells = tr.find_all(["td", "th"])
            if len(cells) != ncols:
                continue
            for j, c in enumerate(cells):
                col_texts[j].append(clean_text(c.get_text("\n")))
        time_counts = [sum(1 for t in col_texts[j] if TIME_PAT_CELL.search(t)) for j in range(ncols)]
        avg_len     = [ (sum(len(t) for t in col_texts[j]) / max(1, len(col_texts[j]))) for j in range(ncols)]
        col_time  = max(range(ncols), key=lambda j: time_counts[j]) if ncols else 1
        candidates = [j for j in range(ncols) if j != col_time]
        col_place = (max(candidates, key=lambda j: avg_len[j]) if candidates else (0 if col_time != 0 else 1))
        col_route = None
        header_rows_used = hdr_rows
        if debug:
            print(f"[디버그] 헤더/내용 탐지: ncols={ncols}, time_counts={time_counts}, avg_len={[round(x,1) for x in avg_len]}")
            print(f"[디버그] col_time={col_time}, col_place={col_place}, col_route={col_route}")
        return col_time, col_place, col_route, header_rows_used

    data_start = 1 if len(trs) > 1 else 0
    if len(trs) <= data_start:
        return 1, 2, None, data_start
    ncols = len(trs[data_start].find_all(["td", "th"]))
    col_texts = [[] for _ in range(ncols)]
    sample_rows = trs[data_start: min(len(trs), data_start + 15)]
    for tr in sample_rows:
        cells = tr.find_all(["td", "th"])
        if len(cells) != ncols:
            continue
        for j, c in enumerate(cells):
            col_texts[j].append(clean_text(c.get_text("\n")))
    time_counts = [sum(1 for t in col_texts[j] if TIME_PAT_CELL.search(t)) for j in range(ncols)]
    avg_len     = [ (sum(len(t) for t in col_texts[j]) / max(1, len(col_texts[j]))) for j in range(ncols)]
    col_time  = max(range(ncols), key=lambda j: time_counts[j]) if ncols else 1
    candidates = [j for j in range(ncols) if j != col_time]
    col_place = (max(candidates, key=lambda j: avg_len[j]) if candidates else (0 if col_time != 0 else 1))
    col_route = None
    if debug:
        print(f"[디버그] 내용기반 탐지: ncols={ncols}, time_counts={time_counts}, avg_len={[round(x,1) for x in avg_len]}")
        print(f"[디버그] col_time={col_time}, col_place={col_place}")
    return col_time, col_place, col_route, data_start

def split_location_and_route(cell_text: str) -> Tuple[str, str]:
    txt = clean_text(cell_text)
    if not txt:
        return "", ""
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    if not lines:
        return txt, ""
    place_lines: List[str] = []
    route_lines: List[str] = []
    for ln in lines:
        if ln.startswith("※") and ("행진" in ln or "이동" in ln):
            route_lines.append(re.sub(r"^※\s*(행진|이동)\s*:\s*", "", ln))
        else:
            place_lines.append(ln)
    tmp_place = []
    for ln in place_lines:
        if "→" in ln or "↔" in ln or "⇄" in ln:
            route_lines.append(ln)
        else:
            tmp_place.append(ln)
    place = normalize_spaces(" ".join(tmp_place)) if tmp_place else (lines[0] if lines else txt)
    route = normalize_spaces(" ".join(route_lines))
    return place, route

def parse_table_rows(table: BeautifulSoup, debug: bool=False) -> List[Dict]:
    trs = table.find_all("tr")
    if len(trs) < 2:
        return []
    col_time, col_place, col_route, header_rows_used = detect_columns(table, debug=debug)
    out: List[Dict] = []
    for tr in trs[header_rows_used:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= max(col_time, col_place):
            continue
        t_raw = clean_text(cells[col_time].get_text("\n"))
        p_raw = clean_text(cells[col_place].get_text("\n"))
        place, route = split_location_and_route(p_raw)
        if not (t_raw or place or route):
            continue
        out.append({"시간": t_raw, "장소": place, "행진경로": route})
    return out

def parse_any(soup: BeautifulSoup, debug: bool=False) -> List[Dict]:
    soup_preprocess(soup)
    table = best_table_by_content(soup, debug=debug)
    if table:
        rows = parse_table_rows(table, debug=debug)
        if rows:
            return rows
    container = soup.select_one("#hwpEditorBoardContent, .detail_contents, .notice_contents, #contents, #container, .content")
    text = container.get_text("\n") if container else soup.get_text("\n")
    text = clean_text(text)
    blocks = []
    for m in TIME_PAT_CELL.finditer(text):
        blocks.append((m.start(), m.group()))
    rows: List[Dict] = []
    for i, (pos, tstr) in enumerate(blocks):
        end = blocks[i+1][0] if i+1 < len(blocks) else len(text)
        body = text[pos:end]
        lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
        rem = [ln for ln in lines if not TIME_PAT_CELL.search(ln)]
        if rem:
            place, route = split_location_and_route("\n".join(rem))
        else:
            place, route = "", ""
        rows.append({"시간": clean_text(tstr), "장소": place, "행진경로": route})
    return rows

def crawl(url: str, debug: bool=False, debug_html_path: Optional[Path]=None) -> List[Dict]:
    try:
        html_text = fetch_html(url)
    except requests.RequestException as e:
        if debug:
            print(f"[디버그] HTTP 오류: {e}")
        return []
    if debug_html_path:
        try:
            debug_html_path.parent.mkdir(parents=True, exist_ok=True)
            debug_html_path.write_text(html_text, encoding="utf-8")
            if debug:
                print(f"[디버그] 원본 HTML 저장: {debug_html_path}")
        except Exception as e:
            if debug:
                print(f"[디버그] HTML 저장 실패: {e}")

    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html_text, parser)
            rows = parse_any(soup, debug=debug)
            if rows:
                return rows
        except FeatureNotFound:
            continue
        except Exception as e:
            if debug:
                print(f"[디버그] 파싱 실패(parser={parser}): {e}")
            continue
    return []

# ---------------- 필터/그룹/CSV ----------------
def is_jongno_related(place: str, route: str) -> bool:
    blob = f"{place}\n{route}".lower()
    return any(kw.lower() in blob for kw in JONGNO_KEYWORDS)

def filter_for_jongno(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        place = r.get("장소", "")
        route = r.get("행진경로", "")
        if is_jongno_related(place, route):
            out.append(r)
    return out

def parse_time_range(t: str) -> Tuple[str, str]:
    t = clean_text(t)
    m = TIME_PAT_CELL.search(t)
    if not m:
        return "", ""
    sh, sm, eh, em = m.group(1), m.group(2), m.group(3), m.group(4)
    start = f"{int(sh):02d}:{int(sm):02d}"
    end = f"{int(eh):02d}:{int(em):02d}" if (eh and em) else ""
    return start, end

def unique_preserve_order(xs: List[str]) -> List[str]:
    seen, out = set(), []
    for x in xs:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

def group_rows_by_time(rows: List[Dict]) -> List[Dict]:
    groups: Dict[Tuple[str,str], Dict] = {}
    for r in rows:
        start, end = parse_time_range(r.get("시간",""))
        place = clean_text(r.get("장소",""))
        remark = clean_text(r.get("행진경로",""))
        key = (start, end)
        if key not in groups:
            groups[key] = {"start_time": start, "end_time": end, "places": [], "remarks": []}
        if place:
            groups[key]["places"].append(place)
        if remark:
            groups[key]["remarks"].append(remark)

    out: List[Dict] = []
    for g in groups.values():
        g["places"] = unique_preserve_order([p for p in g["places"] if p])
        g["remarks"] = unique_preserve_order([rk for rk in g["remarks"] if rk])
        out.append(g)

    def _tkey(g):
        return (g["start_time"] or "99:99", g["end_time"] or "99:99")
    out.sort(key=_tkey)
    return out

# ─────────────── 지오코딩 유틸(정규화/후보/컨텍스트) ───────────────
GU_PATTERN = re.compile(r"(종로구|중구|용산구|성동구|광진구|동대문구|중랑구|성북구|강북구|도봉구|노원구|은평구|서대문구|마포구|양천구|강서구|구로구|금천구|영등포구|동작구|관악구|서초구|강남구|송파구|강동구)")

POLICE_TO_GU = {
    "종로서": "종로구", "남대문서": "중구", "중부서": "중구", "용산서": "용산구",
    "서대문서": "서대문구", "마포서": "마포구", "영등포서": "영등포구", "동작서": "동작구",
    "관악서": "관악구", "금천서": "금천구", "구로서": "구로구", "강서서": "강서구",
    "양천서": "양천구", "강남서": "강남구", "서초서": "서초구", "송파서": "송파구",
    "강동서": "강동구", "동대문서": "동대문구", "성북서": "성북구", "노원서": "노원구",
    "도봉서": "도봉구", "강북서": "강북구", "성동서": "성동구", "광진서": "광진구", "은평서": "은평구",
}

def extract_gu(text: str) -> Optional[str]:
    if not text:
        return None
    m = GU_PATTERN.search(text)
    if m:
        return m.group(1)
    for k, gu in POLICE_TO_GU.items():
        if k in text:
            return gu
    return None

def _to_ascii_digits(s: str) -> str:
    mapping = {
        ord('０'): '0', ord('１'): '1', ord('２'): '2', ord('３'): '3', ord('４'): '4',
        ord('５'): '5', ord('６'): '6', ord('７'): '7', ord('８'): '8', ord('９'): '9',
        ord('〇'): '0',
    }
    return s.translate(mapping)

def _insert_space_between_kor_engnum(s: str) -> str:
    s = re.sub(r'([가-힣])([A-Za-z0-9])', r'\1 \2', s)
    s = re.sub(r'([A-Za-z0-9])([가-힣])', r'\1 \2', s)
    return s

def normalize_tokens_basic(place: str) -> str:
    t = (place or "").strip()
    t = _to_ascii_digits(t)
    t = t.replace("出口", "출구").replace("出", "출구").replace("口", "출구")
    t = _insert_space_between_kor_engnum(t)
    t = re.sub(r'(\d+)\s*(?:번)?\s*(?:출|출구)\b', r'\1번 출구', t)
    t = re.sub(r'(\d+)\s*번\s*출구', r'\1번 출구', t)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t

# === [ADD] 장소 정제/핵심 POI 추출 ===
WING_TOKENS = ["본관","별관","서관","동관","남관","북관","정문","후문"]
NOISE_TOKENS = [
    "앞","맞은편","방면","방향","인근","주변","부근","일대","일원",
    "사거리","교차로","삼거리","부근 일대","사거리 일대"
]

def strip_parentheses_and_noise(s: str) -> str:
    # 괄호류 내용 제거 & 'n개 차로' 등 제거
    s = re.sub(r"[()\[\]{}〈〉＜＞]", " ", s)
    s = re.sub(r"\b\d+\s*개\s*차로\b", " ", s)
    s = re.sub(r"\b\d+\s*차로\b", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def split_core_and_wing(s: str) -> Tuple[str, List[str]]:
    """
    '정곡빌딩 남관 앞' -> core='정곡빌딩', wings=['남관'], 수식어 제거
    """
    tokens = s.split()
    core_parts, wings = [], []
    for tk in tokens:
        if tk in WING_TOKENS:
            wings.append(tk)
        elif tk in NOISE_TOKENS:
            continue
        else:
            core_parts.append(tk)
    core = " ".join(core_parts).strip()
    return core, wings

def expand_wing_synonyms(wings: List[str]) -> List[str]:
    out = set()
    for w in wings:
        out.add(w)
        if w == "남관":
            out.update(["남문","남측 출입문"])
        elif w == "북관":
            out.update(["북문","북측 출입문"])
        elif w == "동관":
            out.update(["동문","동측 출입문"])
        elif w == "서관":
            out.update(["서문","서측 출입문"])
    return list(out)

def add_core_spacing_variants(name: str) -> List[str]:
    # '정곡빌딩' ↔ '정곡 빌딩' 등
    cands = {name}
    cands.add(name.replace("빌딩", " 빌딩"))
    cands.add(name.replace("타워", " 타워"))
    return list(cands)

# --- [REPLACE] 후보 질의 생성기: build_query_candidates ---
def build_query_candidates(place: str, remark: str) -> List[str]:
    # 1) 기본 정규화 + 괄호/차로수 등 노이즈 제거
    base0 = normalize_tokens_basic(place)
    base1 = strip_parentheses_and_noise(base0)

    # 2) 핵심 POI와 동/서/남/북/본/별관 등 분리
    core, wings = split_core_and_wing(base1)
    wing_syns = expand_wing_synonyms(wings)

    # 3) 코어 명칭의 띄어쓰기/표기 변형
    core_vars = add_core_spacing_variants(core) if core else []
    # base 변형도 일부 유지
    base_vars = add_core_spacing_variants(base1) if base1 else []

    # 4) 기본 후보(코어 우선) 구성
    cand: List[str] = []
    seen = set()

    def add(x: str):
        xx = x.strip()
        if not xx or xx in seen:
            return
        seen.add(xx); cand.append(xx)

    for cv in core_vars or []:
        add(cv)
        for w in wings:
            add(f"{cv} {w}")
        for w in wing_syns:
            add(f"{cv} {w}")

    if base1:
        add(base1)
        for v in base_vars:
            add(v)

    # "역 2번 출구" 패턴 유지
    m = re.search(r'(.*?역)\s*(\d+)\s*번\s*출구', base1)
    if m:
        st, num = m.group(1).strip(), m.group(2)
        for v in [f"{st} {num}번 출구", f"{st} {num}번출구", f"{st} {num} 출구", st]:
            add(v)

    # 5) 구 힌트/서울 접두어
    gu = extract_gu(f"{place} {remark}")  # 장소/비고에서 구 추정
    prefixes = []
    if gu: prefixes.append(f"서울 {gu}")
    prefixes.append("서울")

    expanded = []
    for q in cand:
        expanded.append(q)
        for pfx in prefixes:
            expanded.append(f"{pfx} {q}")

    # 6) 중복 제거
    out, seen2 = [], set()
    for q in expanded:
        q2 = re.sub(r'\s{2,}', ' ', q).strip()
        if q2 and q2 not in seen2:
            seen2.add(q2); out.append(q2)

    return out

# === [ADD] VWorld 결과 스코어링 선택 ===
def _choose_best_item(items: list, gu_hint: Optional[str], needle: Optional[str]) -> Optional[dict]:
    """
    VWorld place 검색 결과 중에서 서울/구 힌트/키워드 일치도를 점수화하여 최적 후보 선택
    """
    if not items:
        return None

    def score(it: dict) -> int:
        s = json.dumps(it, ensure_ascii=False).lower()
        sc = 0
        if '서울' in s: sc += 2
        if '종로' in s: sc += 2
        if gu_hint and gu_hint.lower() in s: sc += 3
        if needle and needle.lower() in s: sc += 2  # 키워드(핵심 POI) 매칭
        title = str(it.get('title','')).lower()
        if needle and needle.lower() in title:
            sc += 3
        return sc

    items_sorted = sorted(items, key=lambda x: score(x), reverse=True)
    return items_sorted[0]

# --- [REPLACE] VWorld place 검색(확장+스코어링) ---
def _vworld_search_place(query: str, key: str, session: requests.Session,
                         gu_hint: Optional[str] = None,
                         needle: Optional[str] = None) -> Optional[Tuple[float, float]]:
    params = {
        "service": "search", "request": "search", "version": "2.0", "format": "json",
        "size": 10,  # 더 넉넉히
        "page": 1, "type": "place", "query": query, "key": key
    }
    try:
        r = session.get(VWORLD_SEARCH_URL, params=params, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        items = (((data or {}).get("response") or {}).get("result") or {}).get("items", [])
        if not isinstance(items, list) or not items:
            return None

        best = _choose_best_item(items, gu_hint, needle) or items[0]
        pt = (best.get("point") or {})
        x, y = pt.get("x"), pt.get("y")
        if x is None or y is None:
            coords = ((best.get("geometry") or {}).get("coordinates"))
            if isinstance(coords, list) and len(coords) >= 2:
                x, y = coords[0], coords[1]
        if x is None or y is None:
            return None
        lon = float(x); lat = float(y)
        return (lat, lon)
    except requests.RequestException:
        return None
    except Exception:
        return None

def _vworld_address_coord(addr: str, key: str, session: requests.Session, addr_type: str) -> Optional[Tuple[float, float]]:
    params = {
        "service": "address", "request": "getCoord", "version": "2.0", "format": "json",
        "crs": "EPSG:4326", "type": addr_type, "address": addr, "key": key
    }
    try:
        r = session.get(VWORLD_ADDR_URL, params=params, timeout=6)
        if r.status_code != 200:
            return None
        data = r.json()
        res = (data.get("response") or {}).get("result") or {}
        pt = (res.get("point") or {})
        x, y = pt.get("x"), pt.get("y")
        if x is None or y is None:
            return None
        lon = float(x); lat = float(y)
        return (lat, lon)
    except requests.RequestException:
        return None
    except Exception:
        return None

# --- [REPLACE] geocode_vworld: 힌트 전달 가능 ---
def geocode_vworld(query: str, key: str, session: requests.Session,
                   gu_hint: Optional[str] = None,
                   needle: Optional[str] = None) -> Optional[Tuple[float, float]]:
    q = (query or "").strip()
    if not q:
        return None
    hit = _vworld_search_place(q, key, session, gu_hint=gu_hint, needle=needle)
    if hit: return hit
    hit = _vworld_address_coord(q, key, session, "road")
    if hit: return hit
    return _vworld_address_coord(q, key, session, "parcel")

# --- [REPLACE] geocode_grouped_inplace: 힌트 전달/캐시/속도제어 ---
def geocode_grouped_inplace(grouped: List[Dict], vworld_key: str, sleep_sec: float = 0.15):
    session = requests.Session()
    cache: Dict[str, Optional[Tuple[float, float]]] = {}
    for g in grouped:
        places = g.get("places", []) or []
        remark_context = " | ".join(g.get("remarks", []) or [])
        # 구 힌트 추출
        gu_hint = extract_gu(f"{' '.join(places)} {remark_context}")
        lats: List[Optional[float]] = []
        lons: List[Optional[float]] = []
        for p in places:
            # 코어 키워드(needle) 산출
            core_, _w_ = split_core_and_wing(strip_parentheses_and_noise(normalize_tokens_basic(str(p or ""))))
            candidates = build_query_candidates(str(p or ""), remark_context)
            hit: Optional[Tuple[float, float]] = None
            for q in candidates:
                if q in cache:
                    hit = cache[q]
                else:
                    hit = geocode_vworld(q, vworld_key, session, gu_hint=gu_hint, needle=core_ or None)
                    cache[q] = hit
                    time.sleep(sleep_sec)
                if hit:
                    break
            if hit:
                lat, lon = hit
                lats.append(lat); lons.append(lon)
            else:
                lats.append(None); lons.append(None)
        g["lats"] = lats
        g["lons"] = lons

# ---------------- CSV 렌더링 ----------------
def records_to_csv_rows(ymd: Optional[Tuple[str, str, str]], grouped: List[Dict]) -> List[Dict]:
    """
    최종 CSV 행으로 변환:
      ['년','월','일','start_time','end_time','장소','인원','위도','경도','비고']
    - 장소/위도/경도는 JSON 문자열
    - 지오코딩이 수행되지 않은 경우 위도/경도는 "[]"
    """
    if ymd is not None:
        Y, M, D = ymd
    else:
        Y = M = D = ""

    rows_csv: List[Dict] = []
    for g in grouped:
        places = g.get("places", [])
        places_json = json.dumps(places, ensure_ascii=False)
        if "lats" in g and "lons" in g:
            lat_list = g["lats"]
            lon_list = g["lons"]
        else:
            lat_list = []
            lon_list = []
        lats_json = json.dumps(lat_list, ensure_ascii=False)
        lons_json = json.dumps(lon_list, ensure_ascii=False)
        remarks = " | ".join(g.get("remarks", []))
        rows_csv.append({
            "년": Y, "월": M, "일": D,
            "start_time": g.get("start_time",""),
            "end_time": g.get("end_time",""),
            "장소": places_json,
            "인원": "",
            "위도": lats_json,
            "경도": lons_json,
            "비고": remarks
        })
    return rows_csv

def save_csv_new_schema(records: List[Dict], out_path: str):
    fieldnames = ["년","월","일","start_time","end_time","장소","인원","위도","경도","비고"]
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

# ---------------- 메인 ----------------
def main():
    p = argparse.ArgumentParser(description="SPATIC 집회·통제정보 크롤러 (Selenium 목록 기반 / 새 CSV 스키마 / VWorld 지오코딩)")
    p.add_argument("--url", help="게시글 URL 직접 지정 (지정 시 mgrSeq/목록 무시)")
    p.add_argument("--mgr-seq", type=int, help="mgrSeq 직접 지정 (예: 1177)")
    p.add_argument("--out", default="data/집회_정보.csv", help="저장 CSV 경로 (기본: data/집회_정보.csv)")
    p.add_argument("--debug", action="store_true", help="파싱/선택 근거 로그 출력")
    p.add_argument("--debug-html", action="store_true", help="원본 HTML을 debug/ 폴더에 저장")
    p.add_argument("--vworld-key", default=None, help="VWorld API Key (기본: 환경변수 VWORLD_KEY 또는 내장 기본키)")
    p.add_argument("--rate", type=float, default=0.15, help="지오코딩 호출 간 대기(초), 기본 0.15")
    args = p.parse_args()

    # 키 우선순위: --vworld-key > 환경변수 > DEFAULT_VWORLD_KEY
    env_key = None
    try:
        import os
        env_key = os.environ.get("VWORLD_KEY")
    except Exception:
        env_key = None
    vworld_key = args.vworld_key or env_key or DEFAULT_VWORLD_KEY

    chosen_url: Optional[str] = None
    chosen_post: Optional[Dict] = None   # number/title/date/...

    # 우선순위: --url > --mgr-seq > (Selenium) 목록 기반 자동 선택
    if args.url:
        chosen_url = args.url
        if args.debug:
            print(f"[정보] 요청 URL: {chosen_url} (URL 직접 지정)")
        rows = crawl(chosen_url, debug=args.debug,
                     debug_html_path=(Path("debug") / "manual_url.html") if args.debug_html else None)
        ymd = None  # 목록을 보지 않았으므로 날짜는 공란

    elif args.mgr_seq is not None:
        chosen_url = DETAIL_URL_FMT.format(mgrSeq=args.mgr_seq)
        if args.debug:
            print(f"[정보] 요청 URL: {chosen_url} (mgrSeq 직접 지정: {args.mgr_seq})")
        rows = crawl(chosen_url, debug=args.debug,
                     debug_html_path=(Path("debug") / f"mgrseq_{args.mgr_seq}.html") if args.debug_html else None)
        ymd = None  # 목록을 보지 않았으므로 날짜는 공란

    else:
        if not SELENIUM_AVAILABLE:
            print("[오류] Selenium을 사용할 수 없습니다. --mgr-seq 또는 --url 옵션을 지정해 주세요.")
            return
        posts = get_list_posts(debug=args.debug)
        if args.debug:
            print(f"[디버그] 목록 총 {len(posts)}건 수집")
        event_posts = filter_event_posts(posts, limit=10)
        if args.debug:
            for i, p_ in enumerate(event_posts, 1):
                print(f"[디버그] 이벤트[{i}] 번호={p_.get('number')} / 날짜={p_.get('date')} / 제목={p_.get('title')}")
        chosen_post = select_highest_post(event_posts)
        if not chosen_post:
            print("[오류] 목록 상위 10개에서 '행사 및 집회' 게시글을 찾지 못했습니다.")
            return
        chosen_url = DETAIL_URL_FMT.format(mgrSeq=chosen_post["number"])
        if args.debug:
            print(f"[정보] 목록 기반 선택 mgrSeq={chosen_post['number']} → {chosen_url} / 목록날짜='{chosen_post.get('date','')}'")
        rows = crawl(chosen_url, debug=args.debug,
                     debug_html_path=(Path("debug") / f"list_{chosen_post['number']}.html") if args.debug_html else None)
        ymd = parse_list_date_to_ymd(chosen_post.get("date", ""))

    # 결과 처리
    if not chosen_url:
        print("[오류] 유효한 게시글 URL을 결정하지 못했습니다.")
        return

    # 1) 종로 필터
    rows_filtered = filter_for_jongno(rows)
    # 2) 시간대 그룹핑
    grouped = group_rows_by_time(rows_filtered)
    # 3) 지오코딩 (장소 수만큼 [lat], [lon] 리스트 생성; 실패 None→CSV에서는 null)
    if vworld_key:
        if args.debug:
            tail = ('***' + vworld_key[-6:]) if len(vworld_key or '') > 6 else '***'
            print(f"[정보] VWorld 지오코딩 시행 (rate={args.rate}s, key={tail})")
        geocode_grouped_inplace(grouped, vworld_key, args.rate)
    else:
        if args.debug:
            print("[경고] VWorld 키가 없어 지오코딩을 건너뜁니다. --vworld-key 또는 환경변수 설정 필요.")

    # 4) CSV 저장
    final_records = records_to_csv_rows(ymd, grouped)

    # --- 파일명: 집회 날짜 기준 ---
    if ymd and all(ymd):
        date_str = f"{ymd[0]}-{ymd[1]}-{ymd[2]}"
    else:
        date_str = datetime.now().strftime("%Y-%m-%d")

    out_path = Path(f"data/집회_정보_{date_str}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # --- 기존 파일이 있으면 병합 ---
    existing = load_csv(str(out_path))
    merged = merge_records(existing, final_records)
    save_csv(merged, str(out_path))

    print(
        f"[완료] {out_path} 저장 "
        f"(총 {len(merged)}건, 종로구 필터 적용 / 선택 URL={chosen_url}"
        f" / 날짜={date_str})"
    )

if __name__ == "__main__":
    main()
