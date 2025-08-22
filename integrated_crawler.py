#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMPA(서울경찰청) '오늘의 집회' PDF → CSV
- 오늘자 게시글 PDF 자동 다운로드(--pdf 미지정) 또는 로컬 PDF 지정(--pdf)
- 게시글 제목에서 YYMMDD 추출 → ['년','월','일'] 채움 (로컬 PDF 지정 시 공란 유지)
- PDF 파싱(행 단위)
- VWorld 지오코딩으로 '위도','경도'를 JSON 리스트 문자열로 채움(장소 수와 동일 길이, 미매칭은 null)
- 전체 CSV와 '종로' 필터 CSV(동일 컬럼) 2개 저장

사용 예:
  python smpa_pdf_to_csv.py                         # 자동 다운로드, 지오코딩, 현재 폴더 저장
  python smpa_pdf_to_csv.py --out 집회정보.csv      # 저장 파일명 지정
  python smpa_pdf_to_csv.py --pdf 250822.pdf        # 로컬 PDF 사용
  python smpa_pdf_to_csv.py --vworld-key <키>       # VWorld 키 지정
"""

import re
import os
import csv
import json
import time
import argparse
import pathlib
import urllib.parse
from typing import List, Dict, Optional, Tuple, Any

import requests
from bs4 import BeautifulSoup

# pdfminer.six 필요
try:
    from pdfminer_high_level import extract_text  # intentional failover name
except Exception:
    try:
        from pdfminer.high_level import extract_text
    except ImportError as e:
        raise SystemExit("pdfminer.six가 필요합니다. 설치 후: pip install pdfminer.six") from e

# ──────────────────────────────────────────────────────────────────────
# SMPA(서울경찰청) 목록/첨부 PDF 다운로드
BASE = "https://www.smpa.go.kr"
LIST_URL = f"{BASE}/user/nd54882.do"  # 서울경찰청 > 오늘의 집회


def ensure_dir(p: str):
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def sanitize_filename(name: str, limit: int = 120) -> str:
    safe = re.sub(r'[^\w가-힣\.-]+', '_', name)
    return safe[:limit].strip('._')


def filename_from_cd(cd: str) -> Optional[str]:
    if not cd:
        return None
    m_star = re.search(r"filename\*\s*=\s*[^']*'[^']*'([^;]+)", cd, re.I)
    if m_star:
        return urllib.parse.unquote(m_star.group(1))
    m = re.search(r'filename\s*=\s*"([^"]+)"', cd, re.I)
    if m:
        return m.group(1)
    m2 = re.search(r'filename\s*=\s*([^;]+)', cd, re.I)
    if m2:
        return m2.group(1).strip()
    return None


def _current_title_pattern() -> Tuple[str, str]:
    # 예) "오늘의 집회 250822 금"
    from datetime import datetime
    current_date = datetime.now().strftime("%y%m%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    current_day = weekdays[datetime.now().weekday()]
    return current_date, f"오늘의 집회 {current_date} {current_day}"


def parse_goBoardView(href: str):
    m = re.search(r"goBoardView\('([^']+)'\s*,\s*'([^']+)'\s*,\s*'(\d+)'\)", href)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)


def build_view_urls(board_no: str) -> List[str]:
    return [
        f"{BASE}/user/nd54882.do?View&boardNo={board_no}",
        f"{BASE}/user/nd54882.do?dmlType=View&boardNo={board_no}",
    ]


def extract_ymd_from_title(title: str) -> Optional[Tuple[str, str, str]]:
    """
    제목에서 YYMMDD를 찾아 20YY-MM-DD로 확장하여 ('YYYY','MM','DD') 반환.
    예) '오늘의 집회 250822 금' → ('2025','08','22')
    """
    if not title:
        return None
    m = re.search(r'(\d{2})(\d{2})(\d{2})', title)
    if not m:
        return None
    yy, mm, dd = m.group(1), m.group(2), m.group(3)
    yyyy = f"20{yy}"
    return (yyyy, mm, dd)


def get_today_post_info(session: requests.Session, list_url: str = LIST_URL) -> Tuple[str, str]:
    """
    목록 페이지에서 오늘자 게시글의 뷰 URL과 '제목 텍스트'를 함께 반환.
      return: (view_url, title_text)
    """
    current_date, expected_full = _current_title_pattern()
    r = session.get(list_url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    tbody = soup.select_one("#subContents > div > div.inContent > table > tbody")
    targets = tbody.select("a[href^='javascript:goBoardView']") if tbody \
        else soup.select("a[href^='javascript:goBoardView']")

    target_link = None
    target_title = None
    for a in targets:
        title = a.get_text(strip=True) or (a.find_parent('td').get_text(strip=True) if a.find_parent('td') else "")
        href = a.get("href", "")
        if expected_full in title or f"오늘의 집회 {current_date}" in title:
            target_link = href
            target_title = title
            break

    if not target_link:
        raise RuntimeError("오늘 날짜 게시글을 찾지 못했습니다.")

    parsed = parse_goBoardView(target_link)
    if not parsed:
        raise RuntimeError("goBoardView 인자를 파싱하지 못했습니다.")
    _, _, board_no = parsed

    for url in build_view_urls(board_no):
        resp = session.get(url, timeout=20)
        if resp.ok and "html" in (resp.headers.get("Content-Type") or "").lower():
            return url, (target_title or "")
    raise RuntimeError("View 페이지 요청에 실패했습니다.")


def parse_attach_onclick(a_tag):
    oc = a_tag.get("onclick", "")
    m = re.search(r"attachfileDownload\('([^']+)'\s*,\s*'(\d+)'\)", oc)
    if not m:
        return None
    return m.group(1), m.group(2)


def _is_pdf(resp: requests.Response, first: bytes) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    return first.startswith(b"%PDF-") or "pdf" in ct


def download_from_view(session: requests.Session, view_url: str, out_dir: str) -> str:
    ensure_dir(out_dir)
    r = session.get(view_url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    candidates = []
    for a in soup.find_all("a"):
        oc = a.get("onclick", "")
        if "attachfileDownload" in oc:
            txt = (a.get_text(strip=True) or "").lower()
            if "pdf" in txt or ".pdf" in txt:
                candidates.append(a)
    if not candidates:
        candidates = [a for a in soup.find_all("a") if "attachfileDownload" in (a.get("onclick", "") or "")]

    last_error = None
    for a_tag in candidates:
        parsed = parse_attach_onclick(a_tag)
        if not parsed:
            continue
        path, attach_no = parsed
        download_url = urllib.parse.urljoin(BASE, path)
        try:
            with session.get(download_url, params={"attachNo": attach_no}, stream=True, timeout=30) as resp:
                resp.raise_for_status()
                it = resp.iter_content(chunk_size=8192)
                first_chunk = next(it, b"")
                if not _is_pdf(resp, first_chunk):
                    continue
                cd = resp.headers.get("Content-Disposition", "")
                filename = filename_from_cd(cd) or (a_tag.get_text(strip=True) or f"{attach_no}.pdf")
                root, ext = os.path.splitext(filename)
                if ext.lower() != ".pdf":
                    filename = root + ".pdf"
                filename = sanitize_filename(filename)
                save_path = os.path.join(out_dir, filename)
                with open(save_path, "wb") as f:
                    if first_chunk:
                        f.write(first_chunk)
                    for chunk in it:
                        if chunk:
                            f.write(chunk)
                return save_path
        except Exception as e:
            last_error = e
            continue
    if last_error:
        raise last_error
    raise RuntimeError("PDF 첨부 다운로드에 실패했습니다.")


def download_today_pdf_with_title(out_dir: str = "attachments") -> Tuple[str, str]:
    """
    오늘자 게시글의 PDF를 다운로드하고, '제목 텍스트'를 함께 반환.
      return: (pdf_path, title_text)
    """
    sess = requests.Session()
    sess.headers.update({
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
        'Referer': LIST_URL,
    })
    view_url, title_text = get_today_post_info(sess, LIST_URL)
    pdf_path = download_from_view(sess, view_url, out_dir=out_dir)
    return pdf_path, title_text


# ──────────────────────────────────────────────────────────────────────
# PDF 파서(행 단위)
TIME_RE = re.compile(
    r'(?P<start>\d{1,2}\s*:\s*\d{2})\s*~\s*(?P<end>\d{1,2}\s*:\s*\d{2})',
    re.DOTALL
)

def _normalize_time_breaks(text: str) -> str:
    t = text
    t = re.sub(r'(\d{1,2})\s*\n\s*:\s*(\d{2})', r'\1:\2', t)  # "18\n:00" → "18:00"
    t = re.sub(r'(\d{1,2}\s*:\s*\d{2})\s*\n\s*~\s*\n\s*(\d{1,2}\s*:\s*\d{2})',
               r'\1~\2', t)  # "12:00\n~\n13:30" → "12:00~13:30"
    return t

def _collapse_korean_gaps(s: str) -> str:
    def fix_token(tok: str) -> str:
        core = tok.replace(" ", "")
        if re.fullmatch(r'[가-힣]+', core) and 2 <= len(core) <= 5:
            return core
        return tok
    return " ".join(fix_token(t) for t in s.split())

def _extract_place_nodes(place_text: str) -> List[str]:
    clean = re.sub(r'<[^>]+>', ' ', place_text)  # 보조정보 제거(비고로 이동)
    clean = re.sub(r'\s+', ' ', clean).strip()
    parts = re.split(r'\s*(?:→|↔|~)\s*', clean)  # 경로 구분자
    nodes = [p.strip() for p in parts if p.strip()]
    return nodes

def _extract_headcount(block: str) -> Optional[Tuple[str, Tuple[int, int]]]:
    m = re.search(r'(\d{1,3}(?:,\d{3})*)\s*명', block)
    if m:
        return m.group(1), m.span()
    for m2 in re.finditer(r'(\d{1,3}(?:,\\d{3})*|\\d{3,})', block):
        num = m2.group(1)
        tail = block[m2.end(): m2.end()+1]
        if tail == '出':  # 출구 번호 오검출 방지
            continue
        try:
            val = int(num.replace(',', ''))
            if val >= 100 or (',' in num):
                return num, m2.span()
        except ValueError:
            pass
    return None

def parse_pdf(pdf_path: str, ymd: Optional[Tuple[str, str, str]] = None) -> List[Dict[str, str]]:
    raw = extract_text(pdf_path) or ""
    text = _normalize_time_breaks(raw)

    rows: List[Dict[str, str]] = []
    matches = list(TIME_RE.finditer(text))
    for i, m in enumerate(matches):
        start_t = re.sub(r'\s+', '', m.group('start'))
        end_t   = re.sub(r'\s+', '', m.group('end'))

        start_idx = m.end()
        end_idx = matches[i+1].start() if i+1 < len(matches) else len(text)
        chunk = text[start_idx:end_idx].strip()

        # 인원
        head = _extract_headcount(chunk)
        if head:
            head_str, (h_s, h_e) = head
            head_clean = head_str.replace(',', '')
            before = chunk[:h_s]
            after  = chunk[h_e:]
        else:
            head_clean = ""
            before = chunk
            after  = ""

        # 장소(경로) 및 보조정보
        place_block = before.strip()
        aux_in_place = " ".join(re.findall(r'<([^>]+)>', place_block))
        nodes = _extract_place_nodes(place_block)

        # 비고 = 인원 이후 잔여 + 장소 보조정보
        remark_raw = " ".join(x for x in [after.strip(), aux_in_place.strip()] if x)
        remark = _collapse_korean_gaps(re.sub(r'\s+', ' ', remark_raw)).strip()

        # 장소 컬럼: 1개면 문자열, 2개 이상이면 JSON 리스트 문자열
        if len(nodes) == 0:
            place_col = ""
        elif len(nodes) == 1:
            place_col = nodes[0]
        else:
            place_col = json.dumps(nodes, ensure_ascii=False)

        row = {
            "년": ymd[0] if ymd else "",
            "월": ymd[1] if ymd else "",
            "일": ymd[2] if ymd else "",
            "start_time": start_t,
            "end_time": end_t,
            "장소": place_col,
            "인원": head_clean,   # 숫자만
            "위도": "[]",         # 지오코딩에서 설정됨
            "경도": "[]",         # 지오코딩에서 설정됨
            "비고": remark,
        }
        rows.append(row)

    return rows

# ──────────────────────────────────────────────────────────────────────
# VWorld 지오코딩
DEFAULT_VWORLD_KEY = os.environ.get("VWORLD_KEY", "46AEEE06-EE1D-3C1F-A4A4-E38D578695E8")
VWORLD_SEARCH_URL = "https://api.vworld.kr/req/search"
VWORLD_ADDR_URL   = "https://api.vworld.kr/req/address"

# 서울 경계 박스(대략)
SEOUL_BBOX = (37.413, 37.715, 126.734, 127.269)  # (lat_min, lat_max, lon_min, lon_max)

def in_seoul_bbox(lat: float, lon: float, bbox=SEOUL_BBOX) -> bool:
    if lat is None or lon is None:
        return False
    lat_min, lat_max, lon_min, lon_max = bbox
    return (lat_min <= lat <= lat_max) and (lon_min <= lon <= lon_max)

GU_PATTERN = re.compile(r"(종로구|중구|용산구|성동구|광진구|동대문구|중랑구|성북구|강북구|도봉구|노원구|은평구|서대문구|마포구|양천구|강서구|구로구|금천구|영등포구|동작구|관악구|서초구|강남구|송파구|강동구)")
POLICE_TO_GU = {
    "종로서": "종로구", "남대문서": "중구", "중부서": "중구", "용산서": "용산구", "서대문서": "서대문구",
    "마포서": "마포구", "영등포서": "영등포구", "동작서": "동작구", "관악서": "관악구", "금천서": "금천구",
    "구로서": "구로구", "강서서": "강서구", "양천서": "양천구", "강남서": "강남구", "서초서": "서초구",
    "송파서": "송파구", "강동서": "강동구", "동대문서": "동대문구", "성북서": "성북구", "노원서": "노원구",
    "도봉서": "도봉구", "강북서": "강북구", "성동서": "성동구", "광진서": "광진구", "은평서": "은평구",
}
def extract_gu_from_remark(remark: str) -> Optional[str]:
    if not remark:
        return None
    m = GU_PATTERN.search(remark)
    if m:
        return m.group(1)
    for k, gu in POLICE_TO_GU.items():
        if k in remark:
            return gu
    return None

# '종로' 키워드(지명/건물/역 등)
JONGNO_KEYWORDS = [
    "종로", "종로구", "종로구청",
    "광화문", "광화문광장", "세종문화회관", "정부서울청사", "경복궁",
    "삼청동", "청운동", "부암동", "인사동", "익선동", "계동", "와룡동", "사직로", "율곡로", "자하문로",
    "경복궁역", "광화문역", "안국역", "종각역", "종로3가역", "종로5가역",
    "흥인지문",  # 동대문(흥인지문)
]

def normalize_no_space(s: str) -> str:
    return re.sub(r"\s+", "", s or "")

def text_has_any(text: str, keywords: List[str]) -> bool:
    t = normalize_no_space(text)
    return any(k in t for k in keywords)

def row_matches_jongno(r: Dict[str, str]) -> bool:
    # 1) 비고에서 구 추정
    remark = r.get("비고", "") or ""
    if extract_gu_from_remark(remark) == "종로구":
        return True
    # 2) 비고 키워드
    if text_has_any(remark, JONGNO_KEYWORDS):
        return True
    # 3) 장소(문자열/JSON) 키워드
    place_col = r.get("장소", "") or ""
    place_text = ""
    if place_col.strip().startswith("["):
        try:
            nodes: List[str] = json.loads(place_col)
        except json.JSONDecodeError:
            nodes = []
        place_text = " ".join(nodes)
    else:
        place_text = place_col
    if text_has_any(place_text, JONGNO_KEYWORDS):
        return True
    return False

def filter_rows_jongno(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return [r for r in rows if row_matches_jongno(r)]

# 비고에서 장소 토큰 키워드 추출 (지오코딩 후보 생성용)
CONTEXT_TOKEN_PAT = re.compile(r"([가-힣A-Za-z0-9]{2,}(?:대로|로|길|가|광장|사거리|교차로|역|동|공원|청사|빌딩|센터|주민센터|회관|학교|대학|병원))")
def extract_context_tokens(remark: str) -> List[str]:
    if not remark:
        return []
    toks = CONTEXT_TOKEN_PAT.findall(remark)
    toks = [re.sub(r"\s+", "", t) for t in toks if len(t) <= 12]
    seen, out = set(), []
    for t in toks:
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out

def _to_ascii_digits(s: str) -> str:
    mapping = {ord('０'):'0',ord('１'):'1',ord('２'):'2',ord('３'):'3',ord('４'):'4',
               ord('５'):'5',ord('６'):'6',ord('７'):'7',ord('８'):'8',ord('９'):'9',ord('〇'):'0'}
    return (s or "").translate(mapping)

def _insert_space_between_kor_engnum(s: str) -> str:
    s = re.sub(r'([가-힣])([A-Za-z0-9])', r'\1 \2', s or "")
    s = re.sub(r'([A-Za-z0-9])([가-힣])', r'\1 \2', s)
    return s

def normalize_tokens_basic(place: str) -> str:
    t = _to_ascii_digits(place or "")
    t = t.replace("出口", "출구").replace("出", "출구").replace("口", "출구")
    t = _insert_space_between_kor_engnum(t)
    t = re.sub(r'(\d+)\s*(?:번)?\s*(?:출|출구)\b', r'\1번 출구', t)
    t = re.sub(r'(\d+)\s*번\s*출구', r'\1번 출구', t)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t

def build_query_candidates(place: str, remark: str) -> List[str]:
    base = normalize_tokens_basic(place)
    cand: List[str] = []
    seen = set()

    def add(x: str):
        xx = (x or "").strip()
        if xx and xx not in seen:
            seen.add(xx); cand.append(xx)

    add(base)

    # 역 출구 패턴: "서울역 12번 출구"
    m = re.search(r'(.*?역)\s*(\d+)\s*번\s*출구', base)
    if m:
        st, num = m.group(1).strip(), m.group(2)
        add(f"{st} {num}번 출구"); add(f"{st} {num}번출구"); add(f"{st} {num} 출구"); add(st)

    # PB 확장
    if re.search(r'\bPB\b', base, re.IGNORECASE) or 'PB' in base:
        stub = re.sub(r'\bPB\b', '', base, flags=re.IGNORECASE).strip()
        add(f"{stub} 파출소"); add(f"{stub} 지구대"); add(f"{stub} 경찰박스")

    # '삼각지' 보강
    if '삼각지' in base and '역' not in base:
        add("삼각지역"); add("삼각지 사거리"); add("삼각지 교차로")

    # 프리픽스
    gu = extract_gu_from_remark(remark or "")
    prefixes = []
    if gu:
        prefixes.append(f"서울 {gu}")
    prefixes.append("서울")

    # 비고 토큰 결합
    ctx_toks = extract_context_tokens(remark or "")

    expanded = []
    for q in cand:
        expanded.append(q)
        for pfx in prefixes:
            expanded.append(f"{pfx} {q}")

    for tok in ctx_toks:
        expanded.append(tok)
        expanded.append(f"{tok} {base}")
        expanded.append(f"{base} {tok}")
        for pfx in prefixes:
            expanded.append(f"{pfx} {tok}")
            expanded.append(f"{pfx} {tok} {base}")
            expanded.append(f"{pfx} {base} {tok}")

    out, seen2 = [], set()
    for q in expanded:
        q2 = re.sub(r'\s{2,}', ' ', q).strip()
        if q2 and q2 not in seen2:
            seen2.add(q2); out.append(q2)
    return out

def _vworld_search_place(query: str, key: str, session: requests.Session,
                         context_gu: Optional[str] = None,
                         restrict_seoul: bool = True) -> Optional[Tuple[float, float]]:
    params = {
        "service": "search", "request": "search", "version": "2.0",
        "format": "json", "size": 7, "page": 1, "type": "place", "query": query, "key": key
    }
    try:
        r = session.get(VWORLD_SEARCH_URL, params=params, timeout=6)
        if r.status_code != 200:
            return None
        data = r.json()
        items = (((data or {}).get("response") or {}).get("result") or {}).get("items", [])
        best = None
        best_score = -1
        for it in items:
            pt = (it.get("point") or {})
            x = pt.get("x"); y = pt.get("y")
            if x is None or y is None:
                coords = ((it.get("geometry") or {}).get("coordinates"))
                if isinstance(coords, list) and len(coords) >= 2:
                    x, y = coords[0], coords[1]
            try:
                lon = float(x); lat = float(y)
            except Exception:
                continue

            addr_obj = it.get("address") or {}
            if isinstance(addr_obj, dict):
                addr = addr_obj.get("road") or addr_obj.get("parcel") or ""
            else:
                addr = str(addr_obj or "")
            title = (it.get("title") or "")

            score = 0
            if "서울" in addr or "Seoul" in addr:
                score += 10
            if context_gu and context_gu in addr:
                score += 4
            qkey = re.sub(r"\s+", "", query)
            if qkey and re.sub(r"\s+", "", title).find(qkey) >= 0:
                score += 2
            if in_seoul_bbox(lat, lon):
                score += 5

            if restrict_seoul:
                if ("서울" not in addr and "Seoul" not in addr) and (not in_seoul_bbox(lat, lon)):
                    continue

            if score > best_score:
                best_score = score
                best = (lat, lon)

        return best
    except requests.RequestException:
        return None

def _vworld_address_coord(addr: str, key: str, session: requests.Session, addr_type: str) -> Optional[Tuple[float, float]]:
    params = {
        "service": "address", "request": "getCoord", "version": "2.0",
        "format": "json", "crs": "EPSG:4326", "type": addr_type, "address": addr, "key": key
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
        if not in_seoul_bbox(lat, lon):
            return None
        return (lat, lon)
    except requests.RequestException:
        return None
    except Exception:
        return None

def geocode_vworld(query: str, key: str, session: requests.Session,
                   context_gu: Optional[str] = None,
                   restrict_seoul: bool = True) -> Optional[Tuple[float, float]]:
    q = (query or "").strip()
    if not q:
        return None
    hit = _vworld_search_place(q, key, session, context_gu=context_gu, restrict_seoul=restrict_seoul)
    if hit:
        return hit
    hit = _vworld_address_coord(q, key, session, "road")
    if hit:
        return hit
    hit = _vworld_address_coord(q, key, session, "parcel")
    return hit

def geocode_rows_inplace(rows: List[Dict[str, str]], vworld_key: str,
                         restrict_seoul: bool = True, sleep_sec: float = 0.15):
    session = requests.Session()
    cache: Dict[str, Optional[Tuple[float, float]]] = {}

    for r in rows:
        remark = r.get("비고", "") or ""
        gu = extract_gu_from_remark(remark) or None

        # 장소 리스트 확보: 문자열(1개) 또는 JSON 배열 문자열
        place_col = r.get("장소", "") or ""
        if place_col.strip().startswith("["):
            try:
                nodes: List[str] = json.loads(place_col)
            except json.JSONDecodeError:
                nodes = []
        else:
            nodes = [place_col] if place_col.strip() else []

        lat_list: List[Optional[float]] = []
        lon_list: List[Optional[float]] = []

        for p in nodes:
            base = str(p or "")
            cleaned = re.sub(r"[（(].*?[）)]", "", base).strip()
            candidates = build_query_candidates(cleaned or base, remark)

            hit: Optional[Tuple[float, float]] = None
            for q in candidates:
                if q in cache:
                    hit = cache[q]
                else:
                    hit = geocode_vworld(q, vworld_key, session, context_gu=gu, restrict_seoul=restrict_seoul)
                    cache[q] = hit
                    time.sleep(sleep_sec)
                if hit:
                    lat, lon = hit
                    if (not restrict_seoul) or in_seoul_bbox(lat, lon):
                        break
                    else:
                        hit = None
                        continue

            if hit:
                lat, lon = hit
                lat_list.append(lat); lon_list.append(lon)
            else:
                lat_list.append(None); lon_list.append(None)

        r["위도"] = json.dumps(lat_list, ensure_ascii=False)
        r["경도"] = json.dumps(lon_list, ensure_ascii=False)

# ──────────────────────────────────────────────────────────────────────
# CSV 출력
def write_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    fields = ["년","월","일","start_time","end_time","장소","인원","위도","경도","비고"]
    ensure_dir(os.path.dirname(out_path) or ".")
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ──────────────────────────────────────────────────────────────────────
# 실행부
def main():
    ap = argparse.ArgumentParser(description="SMPA '오늘의 집회' PDF → CSV (파싱+VWorld 지오코딩+종로 필터)")
    ap.add_argument("--pdf", default=None, help="입력 PDF 경로 (미지정 시 오늘자 게시글에서 자동 다운로드)")
    ap.add_argument("--out", default=None, help="전체 CSV 저장 경로(기본: ./집회정보.csv)")
    ap.add_argument("--attachments-dir", default="attachments", help="자동 다운로드 시 PDF 저장 폴더")
    ap.add_argument("--vworld-key", default=DEFAULT_VWORLD_KEY, help="VWorld API Key (기본: 환경변수 VWORLD_KEY 또는 내장 기본값)")
    ap.add_argument("--no-seoul-filter", action="store_true", help="지오코딩 시 서울 경계 박스 필터 끄기")
    ap.add_argument("--geocode-sleep", type=float, default=0.15, help="지오코딩 요청 간 대기(초)")
    args = ap.parse_args()

    ymd: Optional[Tuple[str, str, str]] = None

    # PDF 경로 결정 + 제목에서 날짜 추출
    if args.pdf:
        pdf_path = args.pdf
        if not os.path.isfile(pdf_path):
            raise SystemExit(f"입력 PDF를 찾을 수 없습니다: {pdf_path}")
    else:
        pdf_path, title_text = download_today_pdf_with_title(out_dir=args.attachments_dir)
        print(f"[정보] 오늘자 PDF 다운로드: {pdf_path}")
        ymd = extract_ymd_from_title(title_text)
        if ymd:
            print(f"[정보] 게시글 제목에서 날짜 추출: {ymd[0]}-{ymd[1]}-{ymd[2]}")
        else:
            print("[경고] 제목에서 날짜(YYMMDD)를 찾지 못했습니다. 년/월/일은 공란으로 저장됩니다.")

    # 파싱
    rows = parse_pdf(pdf_path, ymd=ymd)

    # 지오코딩
    restrict = not args.no_seoul_filter
    if args.vworld_key:
        try:
            geocode_rows_inplace(rows, vworld_key=args.vworld_key,
                                 restrict_seoul=restrict, sleep_sec=args.geocode_sleep)
        except Exception as e:
            print(f"⚠️ 지오코딩 실패(건너뜀): {e}")
    else:
        print("ℹ️ VWorld 키가 없어 지오코딩을 건너뜁니다. --vworld-key 또는 환경변수 VWORLD_KEY를 지정하세요.")

    # 저장 경로(전체/종로)
    # 저장 경로(전체/종로)
    save_dir = os.path.join(os.getcwd(), "data")   # 프로젝트 루트의 data 폴더
    ensure_dir(save_dir)

    if args.out:
        filename = os.path.basename(args.out)      # 파일명만 가져오기
        out_all = os.path.join(save_dir, filename)
    else:
        out_all = os.path.join(save_dir, "집회정보_통합.csv")

    root, ext = os.path.splitext(out_all)
    out_jongno = f"{root}_종로{ext or '.csv'}"

    # 저장
    write_csv(rows, out_all)
    rows_jongno = filter_rows_jongno(rows)
    write_csv(rows_jongno, out_jongno)

    print(f"[완료] 전체 CSV 저장: {out_all} (총 {len(rows)}행)")
    print(f"[완료] 종로 필터 CSV 저장: {out_jongno} (총 {len(rows_jongno)}행)")

if __name__ == "__main__":
    main()
