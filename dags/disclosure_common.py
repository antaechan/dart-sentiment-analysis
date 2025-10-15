"""
공시 크롤링 공통 유틸리티 함수들

KIND, DART 등에서 공시 내용을 가져올 때 사용하는 공통 함수들을 제공합니다.
"""

import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import random

# -------------------------
# 공통 헤더/세션
# -------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://kind.krx.co.kr/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# 요청 간 기본 딜레이(정중함) + 지터 범위
_BASE_DELAY_SEC = 0.1  # 최소 대기
_JITTER_SEC = 0.25  # 0 ~ 0.35초 랜덤 추가


def _polite_pause():
    """요청 사이에 기본 딜레이 + 지터."""
    time.sleep(_BASE_DELAY_SEC + random.random() * _JITTER_SEC)


# -------------------------
# 인코딩 감지 & 디코딩
# -------------------------
_META_CHARSET_RE = re.compile(rb'charset\s*=\s*["\']?([\w\-]+)', re.I)


def _smart_text(
    resp: requests.Response, fallback_domain_hint: str | None = None
) -> str:
    """
    응답 바이트에서 meta/http 헤더의 charset을 우선 반영하고,
    없으면 apparent_encoding을 쓰고, 마지막으로 도메인 힌트로 보정.
    """
    raw = resp.content
    enc = None

    # 1) HTTP 헤더 우선
    ct = resp.headers.get("Content-Type", "")
    m = re.search(r"charset=([\w\-]+)", ct, flags=re.I)
    if m:
        enc = m.group(1).lower()

    # 2) <meta ... charset=...> 스캔
    if not enc:
        m2 = _META_CHARSET_RE.search(raw[:4096])  # 초반부만 보면 충분
        if m2:
            enc = m2.group(1).decode("ascii", "ignore").lower()

    # 3) requests의 추정
    if not enc:
        enc = (resp.apparent_encoding or "").lower() or None

    # 4) 도메인 힌트 (KIND 기본 cp949)
    if (
        not enc
        and fallback_domain_hint
        and "kind.krx.co.kr" in fallback_domain_hint.lower()
    ):
        enc = "cp949"

    # 5) 표준화
    if enc in ("euc-kr", "ks_c_5601-1987", "ks_c_5601-1989"):
        enc = "cp949"

    try:
        return raw.decode(enc or "utf-8", errors="strict")
    except Exception:
        # 최후의 보루: cp949 → utf-8 순서로 시도
        for e in (enc, "cp949", "utf-8", "latin1"):
            if not e:
                continue
            try:
                return raw.decode(e, errors="strict")
            except Exception:
                pass
        # 그래도 실패하면 느슨하게
        return raw.decode(enc or "utf-8", errors="replace")


def get_url(url: str, timeout: int = 20, hint: str | None = None) -> str:
    """URL에서 HTML을 가져옵니다. 인코딩을 자동으로 처리합니다."""
    _polite_pause()
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return _smart_text(r, fallback_domain_hint=hint or url)


def normalize_cell_text(tag) -> str:
    """셀 내부의 <br>를 줄바꿈으로 바꾸고, 앞뒤 공백/중복 개행 정리."""
    # 태그 복사 없이 직접 치환
    for br in tag.find_all("br"):
        br.replace_with("\n")

    txt = tag.get_text("\n", strip=True)
    txt = re.sub(r"\r\n?", "\n", txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt


def strip_tags_fallback(html: str) -> str:
    """
    테이블을 찾지 못했을 때 사용하는 폴백 로직.
    전체 텍스트를 추출합니다.
    """
    soup = BeautifulSoup(html, "lxml")

    # 불필요한 태그 제거
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for p in soup.find_all("p"):
        p.insert_before("\n")
        p.insert_after("\n")

    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_viewer_html(url: str, timeout: int = 20) -> str:
    """뷰어 페이지의 HTML을 가져옵니다."""
    return get_url(url, timeout=timeout, hint=url)


# -------------------------
# KIND: acptNo/docNo → searchContents → setPath(...)에서 docLocPath 추출
# -------------------------
def kind_pick_acpt_doc(viewer_html: str) -> tuple[str | None, str | None]:
    """KIND 뷰어 HTML에서 acptNo와 docNo를 추출합니다."""
    soup = BeautifulSoup(viewer_html, "lxml")
    acpt = None
    el = soup.select_one("#acptNo")
    if el and el.has_attr("value"):
        acpt = el["value"].strip() or None

    doc = None
    opt = soup.select_one("#mainDoc option[selected]") or soup.select_one(
        "#mainDoc option:nth-of-type(2)"
    )
    if opt and opt.get("value"):
        doc = opt["value"].split("|", 1)[0].strip() or None
    return acpt, doc


def kind_fetch_doclocpath(
    kind_base_url: str, doc_no: str, timeout: int = 20
) -> str | None:
    """KIND에서 문서 경로를 가져옵니다."""
    sc_url = urljoin(
        kind_base_url, f"/common/disclsviewer.do?method=searchContents&docNo={doc_no}"
    )
    text = get_url(sc_url, timeout=timeout, hint=kind_base_url)

    # setPath('tocLocPath','docLocPath','server',...)
    m = re.search(r"setPath\(\s*['\"][^'\"]*['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,", text)
    if m:
        return m.group(1).strip()
    # 백업: setPath2(...)
    m2 = re.search(
        r"setPath2\(\s*['\"][^'\"]*['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,", text
    )
    return m2.group(1).strip() if m2 else None


# -------------------------
# iframe 백업 로직 (DART/KIND 공통)
# -------------------------
def find_iframe_src(viewer_html: str) -> str | None:
    """뷰어 HTML에서 iframe의 src를 찾습니다."""
    soup = BeautifulSoup(viewer_html, "lxml")
    iframe = soup.find("iframe", id="docViewFrm")
    if iframe:
        src = (iframe.get("src") or "").strip()
        if src:
            return src
    m = re.search(
        r'docViewFrm\s*\.src\s*=\s*[\'"]([^\'"]+)[\'"]', viewer_html, flags=re.I
    )
    if m:
        return m.group(1).strip()
    m2 = re.search(
        r"setPath\(\s*['\"][^'\"]*['\"]\s*,\s*['\"]([^'\"]+)['\"]", viewer_html
    )
    if m2:
        return m2.group(1).strip()
    return None


def extract_table_content(html: str, table_formatter=None) -> str:
    """
    HTML에서 테이블을 찾아 텍스트로 변환합니다.

    Args:
        html: 파싱할 HTML 문자열
        table_formatter: 테이블을 텍스트로 변환하는 함수.
                        BeautifulSoup table 객체를 받아서 문자열을 반환해야 함.
                        None이면 strip_tags_fallback을 바로 사용.

    Returns:
        추출된 텍스트

    처리 순서:
        1) 테이블(id='XFormD50_Form0_Table1') 최우선 검색
        2) 없으면 테이블(id='XFormD1_Form0_Table0') 검색
        3) 없으면 첫 번째 <table> 대상으로 검색
        4) 테이블이 있고 formatter가 제공되면 formatter 사용
        5) 그래도 없거나 실패하면 전체 텍스트 추출(폴백)
    """
    soup = BeautifulSoup(html, "lxml")

    # id 없이 모든 table을 찾는다
    tables = soup.find_all("table")

    # tr 개수가 1개인 table 또는 class에 'nb'가 명시적으로 있을 때만 필터(그 외 class 미정의 OK)
    def _has_nb_class(tbl):
        cls = tbl.get("class")
        return cls is not None and "nb" in cls

    tables = [
        tbl for tbl in tables if len(tbl.find_all("tr")) > 1 and not _has_nb_class(tbl)
    ]
    table = tables[0] if tables else None

    # 4) 테이블이 있고 formatter가 제공되면 사용
    if table and table_formatter:
        try:
            return table_formatter(table)
        except Exception:
            # 테이블 파싱에 실패하면 아래 폴백으로
            pass

    # 5) 테이블이 없거나 실패한 경우: 전체 텍스트 폴백
    return strip_tags_fallback(html)


def _kind_fetch_text_from_docloc(
    kind_base_url: str, doc_loc_path: str, table_formatter=None, timeout: int = 20
) -> str:
    doc_url = urljoin(kind_base_url, doc_loc_path)
    html = get_url(doc_url, timeout=timeout, hint=kind_base_url)
    return extract_table_content(html, table_formatter=table_formatter)


# -------------------------
# 최종 진입점
# -------------------------
def extract_text(url: str, disclosure_type: str, timeout: int = 20) -> str:
    # 순환 참조를 피하기 위해 여기서 import
    from crawl import crawling_function_map

    viewer_html = fetch_viewer_html(url, timeout=timeout)
    table_formatter = crawling_function_map[disclosure_type]

    if "kind.krx.co.kr" in url.lower():
        acpt, doc = kind_pick_acpt_doc(viewer_html)
        if doc:
            doc_loc = kind_fetch_doclocpath(url, doc, timeout=timeout)
            if doc_loc:
                text = _kind_fetch_text_from_docloc(
                    url, doc_loc, table_formatter=table_formatter, timeout=timeout
                )
                if text and len(text) > 50:
                    return text

    return ""
