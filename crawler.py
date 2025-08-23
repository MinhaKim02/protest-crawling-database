#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SPATIC '행사 및 집회' 최신 글 → 상세 표 파싱(지정 CSS 경로 우선) → 장소(리스트) + VWorld 지오코딩(종로/중구 제한) → CSV 저장

- 목록: 스크립트(JSON) / 앵커(href) / Selenium 폴백 3중 수집
- 제목 판별 완화(행사/집회 등 변형 허용), Top10 제한 제거
- 상세: 제공된 CSS 경로(li.notice_contents > div > table) 우선 파싱
- 헤더가 <td>이고 '시 간'처럼 띄어져도 인식되도록 정규화
- 장소 셀 내부의 여러 <p> + '※행진:' 구간까지 합쳐 장소 토큰 추출
- 분리 기호에 ‘⟷’ 포함, 번호마커(①②…), ※, 괄호(거리/차로 등) 제거 보강
- 결과 저장: data/집회정보_통합.csv, data/집회정보_통합_종로.csv (UTF-8-SIG, Append & Dedup & Sort)
"""

import os
import re
import csv
import json
import time
import pathlib
from collections import OrderedDict
from typing import List, Dict, Tuple, Optional
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# --------------------------- Selenium 사용 가능 여부 ---------------------------
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

# ------------------------------- 상수/설정 ------------------------------------
BASE_URL = "https://www.spatic.go.kr"
LIST_URL = f"{BASE_URL}/spatic/main/assem.do"
DETAIL_URL_FMT = f"{BASE_URL}/spatic/assem/getInfoView.do?mgrSeq={{mgrSeq}}"

DATA_DIR = pathlib.Path("data")

DEFAULT_VWORLD_KEY = os.environ.get("VWORLD_KEY", "46AEEE06-EE1D-3C1F-A4A4-E38D578695E8")
VWORLD_SEARCH_URL = "https://api.vworld.kr/req/search"
VWORLD_ADDR_URL   = "https://api.vworld.kr/req/address"

# 종로구 / 중구 대략 BBOX (lon_min, lat_min, lon_max, lat_max)
BBOX = {
    "jongno_tight": (126.95, 37.565, 127.01, 37.605),
    "jongno_loose": (126.94, 37.55, 127.04, 37.62),
    "jung_tight"  : (126.965, 37.548, 127.02, 37.575),
    "jung_loose"  : (126.955, 37.54, 127.04, 37.585),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# ------------------------------ 종로 필터 보조 -------------------------------
JONGNO_KEYWORDS = [
    "종로", "종로구", "광화문", "광화문광장", "세종문화회관", "정부서울청사",
    "경복궁", "안국역", "경복궁역", "광화문역", "종각역", "종로3가역", "종로5가역",
    "사직로", "율곡로", "자하문로", "인사동", "삼청동", "청운동", "부암동"
]

# -------------------------------- 유틸 ---------------------------------------
def ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)

def clean_text(t: str) -> str:
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def normalize_label(s: str) -> str:
    """헤더 레이블 정규화: 공백/개행 제거 → '시 간'도 '시간'으로"""
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    return s.strip()

def parse_date_any(s: str) -> Optional[Tuple[str, str, str]]:
    """
    'YYYY-MM-DD', 'YYYY.MM.DD', 'YYYY년 M월 D일' → ('년','월','일')
    """
    if not s:
        return None
    s = clean_text(s)
    m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", s)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return y, mo.zfill(2), d.zfill(2)
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return y, mo.zfill(2), d.zfill(2)
    return None

def to_yymmdd(y: str, m: str, d: str) -> str:
    return f"{y[-2:]}{m}{d}"

def time_range_to_tuple(s: str) -> Optional[Tuple[str, str]]:
    """
    'HH:MM ~ HH:MM' → ('HH:MM','HH:MM')
    """
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    s = s.replace("∼", "~").replace("〜", "~").replace("–", "-")
    m = re.search(r"(\d{1,2}\s*:\s*\d{2})\s*[~\-]\s*(\d{1,2}\s*:\s*\d{2})", s)
    if not m:
        m = re.search(r"(\d{1,2}:\d{2})[~\-](\d{1,2}:\d{2})", s.replace(" ", ""))
    if m:
        a = re.sub(r"\s*", "", m.group(1))
        b = re.sub(r"\s*", "", m.group(2))
        return a, b
    return None

def normalize_place_text(s: str) -> str:
    """
    장소 텍스트 정리:
    - 번호마커/불릿/특수문자 제거, '※' 제거
    - 괄호 설명(거리/차로/인도 등) 제거
    - 역 '出' 등 특수표기 정리
    """
    if not s:
        return ""
    s = clean_text(s)
    s = re.sub(r"[①②③④⑤⑥⑦⑧⑨⑩■◆▶•∙·�※]", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)   # 괄호 설명 제거
    s = s.replace("出", "")
    s = re.sub(r"[，、･·]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -–—~→↔⟷↦↪>/")

def split_places(s: str) -> List[str]:
    """
    한 셀의 장소 텍스트를 경로/노드로 분리
    - 분리 기호에 '⟷' 포함
    """
    if not s:
        return []
    s = normalize_place_text(s)
    parts = re.split(r"\s*(?:→|↔|⟷|↦|↪|➝|➔|~|〜|∼|-|–|—|/|,|>|▶|⇒)\s*", s)
    parts = [p.strip() for p in parts if p and p.strip()]
    # 노이즈 컷 + 중복 제거
    filtered = []
    for p in parts:
        if len(p) <= 1 and not re.search(r"[가-힣A-Za-z]", p):
            continue
        filtered.append(p)
    seen, ordered = set(), []
    for p in filtered:
        if p not in seen:
            ordered.append(p)
            seen.add(p)
    return ordered

def in_bbox(lon: float, lat: float, box: Tuple[float, float, float, float]) -> bool:
    lon_min, lat_min, lon_max, lat_max = box
    return (lon_min <= lon <= lon_max) and (lat_min <= lat <= lat_max)

def match_in_jongno_jung(address_str: str) -> bool:
    if not address_str:
        return False
    return ("종로구" in address_str) or ("중구" in address_str)

# ------------------------------ 제목 판별 ------------------------------------
def is_event_title(title: str) -> bool:
    """
    '행사 및 집회', '행사/집회', '행사 ‧ 집회' 등 변형 허용
    """
    if not title:
        return False
    t = re.sub(r"\s+", " ", title).strip()
    if re.search(r"행사\s*(?:및|/|‧|\||,)\s*집회", t):
        return True
    return ("행사" in t) and ("집회" in t)

# ------------------------------ VWorld 지오코딩 -------------------------------
def vworld_search_place(session: requests.Session, query: str, key: str) -> List[Dict]:
    params = {
        "service": "search",
        "request": "search",
        "version": "2.0",
        "crs": "EPSG:4326",
        "size": 20,
        "page": 1,
        "format": "json",
        "type": "place",
        "query": query,
        "key": key
    }
    r = session.get(VWORLD_SEARCH_URL, params=params, timeout=10)
    r.raise_for_status()
    js = r.json()
    items = js.get("response", {}).get("result", {}).get("items", [])
    return items if isinstance(items, list) else []

def vworld_address_geocode(session: requests.Session, address: str, key: str) -> Optional[Tuple[float, float, str]]:
    """
    주소 → (lon, lat, addr)
    """
    params = {
        "service": "address",
        "request": "search",
        "version": "2.0",
        "crs": "EPSG:4326",
        "format": "json",
        "type": "road",
        "address": address,
        "key": key
    }
    r = session.get(VWORLD_ADDR_URL, params=params, timeout=10)
    r.raise_for_status()
    js = r.json().get("response", {})
    if js.get("status") == "OK" and js.get("result", []):
        res = js["result"][0]
        x = float(res["point"]["x"])  # lon
        y = float(res["point"]["y"])  # lat
        addr_out = res.get("text", address)
        return x, y, addr_out

    params["type"] = "parcel"
    r = session.get(VWORLD_ADDR_URL, params=params, timeout=10)
    r.raise_for_status()
    js = r.json().get("response", {})
    if js.get("status") == "OK" and js.get("result", []):
        res = js["result"][0]
        x = float(res["point"]["x"])
        y = float(res["point"]["y"])
        addr_out = res.get("text", address)
        return x, y, addr_out
    return None

def pick_best_points_from_items(items: List[Dict]) -> List[Tuple[float, float, str]]:
    """
    place 검색 결과 → [(lon, lat, addr), ...]
    """
    cands = []
    for it in items:
        try:
            x = float(it["point"]["x"])  # lon
            y = float(it["point"]["y"])  # lat
            addr = it.get("road", {}).get("addr") or it.get("parcel", {}).get("addr") or ""
            cands.append((x, y, addr))
        except Exception:
            continue
    return cands

def geocode_one_place(session: requests.Session, place: str, key: str) -> Tuple[Optional[float], Optional[float]]:
    """
    단일 장소 문자열을 종로/중구 영역으로 지오코딩해 (위도, 경도)를 반환. 실패 시 (None, None)
    반환 순서: (lat, lon)
    """
    place = place.strip()
    if not place:
        return None, None

    # 1) place 검색 (종로/중구 우선)
    queries = [
        f"서울 종로구 {place}",
        f"서울 중구 {place}",
        f"서울 {place}",
        place,
    ]
    for q in queries:
        try:
            items = vworld_search_place(session, q, key)
        except Exception:
            items = []

        cands = pick_best_points_from_items(items)

        # (A) 주소에 종로/중구 포함 + tight BBOX
        for (lon, lat, addr) in cands:
            if match_in_jongno_jung(addr) and (
                in_bbox(lon, lat, BBOX["jongno_tight"]) or in_bbox(lon, lat, BBOX["jung_tight"])
            ):
                return lat, lon

        # (B) 주소에 종로/중구 포함 + loose BBOX
        for (lon, lat, addr) in cands:
            if match_in_jongno_jung(addr) and (
                in_bbox(lon, lat, BBOX["jongno_loose"]) or in_bbox(lon, lat, BBOX["jung_loose"])
            ):
                return lat, lon

        # (C) 주소 매칭 실패 시 BBOX만 일치(느슨)
        for (lon, lat, _addr) in cands:
            if in_bbox(lon, lat, BBOX["jongno_loose"]) or in_bbox(lon, lat, BBOX["jung_loose"]):
                return lat, lon

        time.sleep(0.12)

    # 2) 주소 지오코딩 시도
    addr_trials = [
        f"서울특별시 종로구 {place}",
        f"서울특별시 중구 {place}",
        f"서울특별시 {place}",
    ]
    for a in addr_trials:
        try:
            res = vworld_address_geocode(session, a, key)
        except Exception:
            res = None
        if res:
            lon, lat, addr = res
            if match_in_jongno_jung(addr) and (
                in_bbox(lon, lat, BBOX["jongno_loose"]) or in_bbox(lon, lat, BBOX["jung_loose"])
            ):
                return lat, lon
        time.sleep(0.1)

    return None, None

# ----------------------------- 목록/상세 파싱 --------------------------------
def fetch_list(session: requests.Session) -> List[Dict]:
    """
    목록 HTML에서 게시물 리스트를 얻는다.
    1) 스크립트 내 JSON 수색
    2) 앵커의 href에서 mgrSeq 직접 수집
    3) Selenium 폴백
    """
    posts: List[Dict] = []

    try:
        r = session.get(LIST_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[오류] 목록 요청 실패: {e}")
        return posts

    soup = BeautifulSoup(r.text, "html.parser")

    # 1) 스크립트 내 JSON 수색(공격적으로)
    for sc in soup.find_all("script"):
        s = sc.get_text() or ""
        if not s:
            continue
        patterns = [
            r"var\s+\w*[Ll]ist\w*\s*=\s*(\[.*?\]);",
            r"\w*[Dd]ata\w*\s*=\s*(\[.*?\]);",
            r'resultList["\']?\s*:\s*(\[.*?\])',
            r"(\[{[^}]*['\"]mgrSeq['\"][^}]*}[^]]*\])",
        ]
        for pat in patterns:
            m = re.search(pat, s, re.DOTALL)
            if m:
                try:
                    arr = json.loads(m.group(1))
                    if isinstance(arr, list):
                        for it in arr:
                            if not isinstance(it, dict):
                                continue
                            posts.append({
                                "number": str(it.get("mgrSeq", it.get("id", it.get("seq", "")))),
                                "title": it.get("title", it.get("subject", "")),
                                "date": it.get("regDt", it.get("regDate", it.get("date", ""))),
                                "views": str(it.get("hitCnt", it.get("viewCnt", it.get("views", "0")))),
                                "is_new": (it.get("newYn", "N") == "Y"),
                            })
                except Exception:
                    continue

    # 2) 앵커의 href에서 mgrSeq 직접 수집(+ 같은 행에서 날짜 추정)
    for a in soup.select('a[href*="getInfoView.do?mgrSeq="]'):
        href = a.get("href", "")
        m = re.search(r"mgrSeq=(\d+)", href)
        if not m:
            continue
        mgr = m.group(1)
        title = clean_text(a.get_text(" ", strip=True))
        date_txt = ""
        tr = a.find_parent("tr")
        if tr:
            tds = tr.find_all("td")
            for td in tds:
                tt = clean_text(td.get_text(" ", strip=True))
                if parse_date_any(tt):
                    date_txt = tt
                    break
        posts.append({
            "number": mgr,
            "title": title,
            "date": date_txt,
            "views": "",
            "is_new": False,
        })

    # 3) Selenium 폴백
    if not posts and SELENIUM_AVAILABLE:
        try:
            opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            drv = webdriver.Chrome(options=opts)
            drv.get(LIST_URL)
            for _ in range(12):
                try:
                    rows = drv.find_elements("css selector", "tbody.assem_content tr")
                    if rows:
                        break
                except Exception:
                    pass
                time.sleep(0.5)
            rows = drv.find_elements("css selector", "tbody.assem_content tr")
            for row in rows:
                tds = row.find_elements("css selector", "td")
                if len(tds) >= 3:
                    num = row.get_attribute("key") or tds[0].text.strip()
                    title = tds[1].text.strip()
                    date_txt = tds[2].text.strip()
                    posts.append({
                        "number": num,
                        "title": title,
                        "date": date_txt,
                        "views": tds[3].text.strip() if len(tds) > 3 else "",
                        "is_new": "new" in (row.get_attribute("class") or ""),
                    })
        except Exception as e:
            print(f"[경고] Selenium 폴백 실패: {e}")
        finally:
            try:
                drv.quit()
            except Exception:
                pass

    # 중복 제거(번호 기준)
    uniq: Dict[str, Dict] = {}
    for p in posts:
        k = p.get("number", "")
        if k and k not in uniq:
            uniq[k] = p
    result = list(uniq.values())
    print(f"[정보] 목록 수집: {len(result)}건")
    return result

def select_latest_mgrseq_and_date(posts: List[Dict]) -> Optional[Tuple[int, Tuple[str, str, str]]]:
    """
    '행사 및 집회'만 필터 → 가장 큰 mgrSeq 선택, 그 게시일을 년/월/일로 파싱
    (상위 10개 제한 없음)
    """
    if not posts:
        return None

    event_posts = [p for p in posts if is_event_title(p.get("title", ""))]
    if not event_posts:
        return None

    nums: List[Tuple[int, Dict]] = []
    for p in event_posts:
        m = re.search(r"\d+", p.get("number", ""))
        if m:
            nums.append((int(m.group(0)), p))

    if not nums:
        return None

    nums.sort(key=lambda x: x[0], reverse=True)
    mgr, post = nums[0]
    ymd = parse_date_any(post.get("date", "")) or parse_date_any(datetime.now().strftime("%Y-%m-%d"))
    return mgr, ymd

def fetch_detail_html(session: requests.Session, mgrSeq: int) -> str:
    url = DETAIL_URL_FMT.format(mgrSeq=mgrSeq)
    r = session.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.text

# ----------- 상세 표 찾기: 제공된 CSS 경로 우선 + 폴백 ------------------------
DETAIL_TABLE_SELECTORS = [
    "div.police_main_wrap.detail.flex.flex_column > section > div > div > ul.notice_datail.flex.flex_wrap > li.notice_contents > div > table",
    "ul.notice_datail.flex.flex_wrap li.notice_contents > div > table",
    "li.notice_contents > div > table",
]

def find_detail_table_node(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    # 1) 제공된 CSS 경로 우선
    for sel in DETAIL_TABLE_SELECTORS:
        node = soup.select_one(sel)
        if node and node.name == "table":
            return node
    # 2) 폴백: notice_contents 영역 내 첫 번째 table
    nc = soup.select_one("li.notice_contents")
    if nc:
        t = nc.select_one("table")
        if t:
            return t
    # 3) 최종 폴백: 모든 table 중 '시간/장소' 키워드가 보이는 첫 표
    for tb in soup.find_all("table"):
        txt = normalize_label(tb.get_text(" ", strip=True))
        if ("시간" in txt and "장소" in txt) or ("집결" in txt and "장소" in txt):
            return tb
    return None

# ----------- 헤더 매핑/장소 추출(※행진 포함) + 상세 파싱 ----------------------
def header_index_map_from_row(tr) -> Dict[str, int]:
    cells = tr.find_all(["th", "td"])
    labels = [normalize_label(c.get_text(" ", strip=True)) for c in cells]
    hmap: Dict[str, int] = {}
    for i, lab in enumerate(labels):
        if ("시간" in lab) or ("집회시간" in lab):
            hmap["time"] = i
        if ("장소" in lab) or ("집회장소" in lab) or ("집결" in lab):
            hmap["place"] = i
        if ("행진" in lab) or ("경로" in lab):
            hmap["route"] = i
    return hmap

def extract_places_from_cell(td) -> List[str]:
    """
    장소 셀 내부에 여러 <p>/<span>과 '※행진:' 라인이 공존.
    모든 텍스트를 모아 토큰 분리.
    """
    parts = []
    # 우선 p/span 단위로 긁고, 없으면 셀 전체 텍스트 사용
    for p in td.find_all(["p", "span"]):
        txt = p.get_text(" ", strip=True)
        if txt:
            parts.append(txt)
    if not parts:
        parts = [td.get_text(" ", strip=True)]

    place_raw = " ".join(parts)
    # '※행진:' 라벨/변형 제거
    place_raw = (place_raw
                 .replace("※ 행진 :", " ")
                 .replace("※ 행진:", " ")
                 .replace("※행진 :", " ")
                 .replace("※행진:", " ")
                 .replace("※  행진:", " "))
    tokens = split_places(place_raw)
    return tokens

def parse_detail_to_groups(html: str) -> Dict[Tuple[str, str], List[str]]:
    """
    상세 HTML → {(start, end): [place1, place2, ...]}
    - 제공된 CSS 경로의 표를 우선 파싱
    - 헤더는 <td>여도 인식(공백 제거 후 '시간/장소' 탐지)
    - 장소 셀의 다중 <p> 및 '※행진:' 구간까지 합쳐 분리
    """
    soup = BeautifulSoup(html, "html.parser")
    tb = find_detail_table_node(soup)
    if not tb:
        return {}

    body = tb.find("tbody") or tb
    rows = body.find_all("tr")
    if not rows:
        return {}

    # 1행을 헤더로 가정 (연번/시간/장소 형태)
    hmap = header_index_map_from_row(rows[0])
    if "time" not in hmap or "place" not in hmap:
        # 위치 기반 폴백 (보통 0=연번, 1=시간, 2=장소)
        first_cells_cnt = len(rows[0].find_all(["td", "th"]))
        hmap.setdefault("time", 1 if first_cells_cnt >= 2 else 0)
        hmap.setdefault("place", 2 if first_cells_cnt >= 3 else 1)

    groups: "OrderedDict[Tuple[str, str], List[str]]" = OrderedDict()

    # 데이터 행은 헤더 다음부터
    for tr in rows[1:]:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        # 시간
        time_text = ""
        if hmap["time"] < len(tds):
            time_text = tds[hmap["time"]].get_text(" ", strip=True)
        trange = time_range_to_tuple(time_text) or time_range_to_tuple(" ".join([td.get_text(" ", strip=True) for td in tds]))
        if not trange:
            continue
        start, end = trange

        # 장소(셀 내부 p/행진 포함)
        place_tokens: List[str] = []
        if hmap["place"] < len(tds):
            place_tokens = extract_places_from_cell(tds[hmap["place"]])
        if not place_tokens:
            place_tokens = split_places(" ".join([td.get_text(" ", strip=True) for td in tds]))
        if not place_tokens:
            continue

        key = (start, end)
        if key not in groups:
            groups[key] = []
        for p in place_tokens:
            if p not in groups[key]:
                groups[key].append(p)

    return groups

# -------------------------- CSV 저장/필터 보조 함수 ---------------------------
FIELDS = ["년", "월", "일", "start_time", "end_time", "장소", "인원", "위도", "경도", "비고"]

# ====== (추가) 통합 파일 Append & Dedup(부분 병합) & Sort 유틸 ======
def _json_list(x):
    try:
        v = json.loads(x)
        if isinstance(v, list):
            return v
        if isinstance(v, str) and v.strip():
            return [v]
        return []
    except Exception:
        return [x] if isinstance(x, str) and x.strip() else []

def _canon_tok(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[\"'“”‘’`·ㆍ∙·,，、･]+", "", s)
    s = s.replace("번출구", "번")
    s = re.sub(r"(역)(\d+)$", r"\1\2번", s)  # '광화문역2' -> '광화문역2번'
    return s

def _places_to_set_for_overlap(places_field: str) -> set:
    toks = _json_list(places_field)
    return set(_canon_tok(t) for t in toks if str(t).strip())

def _row_time_key(r: Dict) -> tuple:
    return (r.get("년",""), r.get("월",""), r.get("일",""),
            r.get("start_time",""), r.get("end_time",""))

def update_or_append_time_only(path: pathlib.Path, new_rows: List[Dict]) -> tuple:
    """
    같은 (년,월,일,start_time,end_time)이면 같은 행사로 보고 무시.
    기존 행은 그대로 두고, 없으면 새로 추가.
    반환: (added, skipped)
    """
    ensure_dir(path.parent)

    # 1) 기존 로드
    existing_rows: List[Dict] = []
    if path.exists():
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                existing_rows = list(csv.DictReader(f))
        except Exception:
            existing_rows = []

    # 2) 시간키 인덱스
    time_index = {_row_time_key(er): i for i, er in enumerate(existing_rows)}

    added, skipped = 0, 0

    # 3) 신규 행 처리
    for nr in new_rows:
        tkey = _row_time_key(nr)
        if tkey in time_index:
            skipped += 1   # 같은 시간키 있으면 그냥 패스
        else:
            existing_rows.append(nr)
            time_index[tkey] = len(existing_rows)-1
            added += 1

    # 4) 파일 저장
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in existing_rows:
            w.writerow(r)

    return added, skipped

def _build_place_coord_map(row: Dict) -> "OrderedDict[str, tuple]":
    """canon_place -> (original_place, lat, lon)"""
    places = _json_list(row.get("장소","[]"))
    lats   = _json_list(row.get("위도","[]"))
    lons   = _json_list(row.get("경도","[]"))
    while len(lats) < len(places): lats.append(None)
    while len(lons) < len(places): lons.append(None)
    m = OrderedDict()
    for p, la, lo in zip(places, lats, lons):
        c = _canon_tok(str(p))
        la = None if la in (None, "null", "", "None") else la
        lo = None if lo in (None, "null", "", "None") else lo
        if c and c not in m:
            m[c] = (str(p), la, lo)
    return m

def _merge_place_coord_into(existing_row: Dict, new_row: Dict) -> None:
    """기존 행을 in-place로 갱신: 장소/위도/경도만 합침(기존값 우선, 기존 None이면 새값 이용)"""
    emap = _build_place_coord_map(existing_row)
    nmap = _build_place_coord_map(new_row)

    for c, (orig, lat, lon) in nmap.items():
        if c in emap:
            eorig, elat, elon = emap[c]
            if (elat is None) and (lat is not None): elat = lat
            if (elon is None) and (lon is not None): elon = lon
            emap[c] = (eorig, elat, elon)
        else:
            emap[c] = (orig, lat, lon)

    places_merged, lats_merged, lons_merged = [], [], []
    for (orig, la, lo) in emap.values():
        places_merged.append(orig)
        lats_merged.append(la)
        lons_merged.append(lo)

    existing_row["장소"] = json.dumps(places_merged, ensure_ascii=False)
    existing_row["위도"] = json.dumps(lats_merged, ensure_ascii=False)
    existing_row["경도"] = json.dumps(lons_merged, ensure_ascii=False)

def update_or_append_with_soft_merge(path: pathlib.Path, new_rows: List[Dict], min_common:int=2) -> tuple:
    """
    파일을 읽어, (같은 날짜/시간) + (겹치는 장소 canon 토큰 수 >= min_common)이면
    기존 행을 갱신하고, 아니면 새로 추가. (added, updated) 반환
    """
    ensure_dir(path.parent)

    existing_rows: List[Dict] = []
    if path.exists():
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                rdr = csv.DictReader(f)
                existing_rows = list(rdr)
        except Exception:
            existing_rows = []

    time_index = {}
    for i, er in enumerate(existing_rows):
        time_index.setdefault(_row_time_key(er), []).append(i)

    added, updated = 0, 0

    for nr in new_rows:
        tkey = _row_time_key(nr)
        candidates = time_index.get(tkey, [])
        merged = False

        A = _places_to_set_for_overlap(nr.get("장소","[]"))

        for idx in candidates:
            er = existing_rows[idx]
            B = _places_to_set_for_overlap(er.get("장소","[]"))
            if len(A & B) >= min_common:
                _merge_place_coord_into(er, nr)
                updated += 1
                merged = True
                break

        if not merged:
            existing_rows.append(nr)
            time_index.setdefault(tkey, []).append(len(existing_rows)-1)
            added += 1

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in existing_rows:
            w.writerow(r)

    return added, updated

def _any_coord_in_jongno(lat_list: List[Optional[float]], lon_list: List[Optional[float]]) -> bool:
    for lat, lon in zip(lat_list, lon_list):
        if lat is None or lon is None:
            continue
        if in_bbox(lon, lat, BBOX["jongno_tight"]) or in_bbox(lon, lat, BBOX["jongno_loose"]):
            return True
    return False

def _places_hit_jongno_keywords(places: List[str]) -> bool:
    joined = re.sub(r"\s+", "", " ".join(places))
    return any(k in joined for k in JONGNO_KEYWORDS)

def filter_rows_jongno(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows:
        try:
            lats = json.loads(r.get("위도", "[]"))
            lons = json.loads(r.get("경도", "[]"))
            places = json.loads(r.get("장소", "[]"))
            if isinstance(places, str):
                places = [places] if places else []
        except Exception:
            lats, lons, places = [], [], []

        hit = _any_coord_in_jongno(lats, lons) or _places_hit_jongno_keywords(places)
        if hit:
            out.append(r)
    return out

def _time_to_minutes(t: str) -> int:
    try:
        h, m = (t or "").split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 99999

def sort_csv_inplace(path: pathlib.Path) -> None:
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception:
        return

    def keyfn(r: Dict) -> tuple:
        def to_i(x):
            try: return int(str(x).strip())
            except: return 0
        return (
            to_i(r.get("년","0")),
            to_i(r.get("월","0")),
            to_i(r.get("일","0")),
            _time_to_minutes(r.get("start_time","")),
            _time_to_minutes(r.get("end_time","")),
        )

    rows.sort(key=keyfn)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# --------------------------------- 메인 --------------------------------------
def main():
    ensure_dir(DATA_DIR)

    with requests.Session() as session:
        session.headers.update(HEADERS)

        # 1) 목록 수집 → 최신 '행사 및 집회' mgrSeq 선택 + 게시일(Y/M/D)
        posts = fetch_list(session)
        sel = select_latest_mgrseq_and_date(posts)
        if not sel:
            print("⚠️ 최신 '행사 및 집회' 게시글을 찾지 못했습니다.")
            return
        mgrSeq, ymd = sel
        Y, M, D = ymd
        print(f"[정보] 선택된 mgrSeq={mgrSeq}, 게시일={Y}-{M}-{D}")

        # 2) 상세 페이지 파싱
        html = fetch_detail_html(session, mgrSeq)
        dbg = BeautifulSoup(html, "html.parser")
        print(f"[디버그] 상세 테이블 발견 여부: {bool(find_detail_table_node(dbg))}")

        groups = parse_detail_to_groups(html)
        if not groups:
            print("⚠️ 상세 표 파싱 결과가 비어 있습니다.")
            return

        # 3) 그룹별 장소 지오코딩(종로/중구 제한)
        rows_all: List[Dict] = []
        vkey = DEFAULT_VWORLD_KEY

        for (start, end), places in groups.items():
            lat_list: List[Optional[float]] = []
            lon_list: List[Optional[float]] = []

            for p in places:
                lat, lon = geocode_one_place(session, p, vkey)  # (위도, 경도)
                lat_list.append(lat)
                lon_list.append(lon)
                time.sleep(0.1)  # API rate 완화

            row = {
                "년": Y,
                "월": M,
                "일": D,
                "start_time": start,
                "end_time": end,
                "장소": json.dumps(places, ensure_ascii=False),
                "인원": "",
                "위도": json.dumps(lat_list, ensure_ascii=False),
                "경도": json.dumps(lon_list, ensure_ascii=False),
                "비고": ""
            }
            rows_all.append(row)

        # 4) 저장: 통합 파일에 Append & Dedup(soft-merge) & Sort
        out_all = DATA_DIR / "집회정보_통합.csv"
        out_jongno = DATA_DIR / "집회정보_통합_종로.csv"

        rows_jongno = filter_rows_jongno(rows_all)

        added_all, skipped_all = update_or_append_time_only(out_all, rows_all)
        added_jongno, skipped_jongno = update_or_append_time_only(out_jongno, rows_jongno)

        sort_csv_inplace(out_all)
        sort_csv_inplace(out_jongno)

        print(f"✅ 저장 완료: {out_all.resolve()} (+{added_all} / 스킵 {skipped_all} / 입력 {len(rows_all)})")
        print(f"✅ 저장 완료: {out_jongno.resolve()} (+{added_jongno} / 스킵 {skipped_jongno} / 입력 {len(rows_jongno)})")

if __name__ == "__main__":
    main()
