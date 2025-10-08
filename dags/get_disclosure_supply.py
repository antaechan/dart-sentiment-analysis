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


def _normalize_cell_text(tag) -> str:
    """셀 내부의 <br>를 줄바꿈으로 바꾸고, 앞뒤 공백/중복 개행 정리."""
    # 태그 복사 없이 직접 치환
    for br in tag.find_all("br"):
        br.replace_with("\n")

    txt = tag.get_text("\n", strip=True)
    txt = re.sub(r"\r\n?", "\n", txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt


def _format_rows_from_table(table: BeautifulSoup) -> str:
    """요구사항에 맞게 tr/td 구조를 텍스트로 정제한다."""
    rows = table.find_all("tr")
    output_lines = []

    for tr in rows:
        # td/th 모두 허용(간혹 th가 들어오는 공시가 있음)
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        # 각 셀 텍스트 정규화
        tds = [_normalize_cell_text(td) for td in cells]

        # 첫 번째 셀은 메인 항목
        main = tds[0].strip()
        if not main:
            # 메인 라벨이 비면 해당 행 스킵
            continue

        # 나머지 셀 처리 규칙
        if len(tds) == 1:
            # 단독 항목 행
            output_lines.append(main)
        elif len(tds) == 2:
            # "항목: 값" 형태
            output_lines.append(f"{main}: {tds[1]}")
        else:
            # 세부 칼럼 구조
            details = []
            # 두 번째 이후 셀들을 2개씩 묶어 "이름: 값"
            i = 1
            while i < len(tds):
                name = tds[i]
                value = ""
                if i + 1 < len(tds):
                    value = tds[i + 1]
                    # 이름이 공백이거나 '-' 만 있을 수도 있음 → 그럴 땐 그냥 값만
                    if name and name != "-":
                        details.append(f"{name}: {value}")
                    else:
                        details.append(value)
                    i += 2
                else:
                    # 마지막 홀수 개 남은 경우
                    details.append(name)
                    i += 1

            # 메인 라벨 + 세부라인 묶기
            if details:
                output_lines.append(main)
                output_lines.extend(details)
            else:
                # 혹시라도 details가 비면 메인만 출력
                output_lines.append(main)

    # 최종 합치기
    return "\n".join(output_lines)


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


def _get(url: str, timeout: int = 20, hint: str | None = None) -> str:
    _polite_pause()
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return _smart_text(r, fallback_domain_hint=hint or url)


def _strip_tags(html: str) -> str:
    """
    1) 테이블(id='XFormD1_Form0_Table0') 우선 정제
    2) 없으면 첫 번째 <table> 대상으로 정제
    3) 그래도 없으면 전체 텍스트 추출(폴백)
    """
    soup = BeautifulSoup(html, "lxml")

    # 1) 명시 테이블 우선
    table = soup.find("table", {"id": "XFormD1_Form0_Table0"})
    if not table:
        # 2) 첫 번째 테이블 폴백
        all_tables = soup.find_all("table")
        if all_tables:
            table = all_tables[0]

    if table:
        try:
            return _format_rows_from_table(table)
        except Exception:
            # 테이블 파싱에 실패하면 아래 폴백으로
            pass

    # 3) 테이블이 없거나 실패한 경우: 전체 텍스트 폴백
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
    return _get(url, timeout=timeout, hint=url)


# -------------------------
# KIND: acptNo/docNo → searchContents → setPath(...)에서 docLocPath 추출
# -------------------------
def _kind_pick_acpt_doc(viewer_html: str) -> tuple[str | None, str | None]:
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


def _kind_fetch_doclocpath(
    kind_base_url: str, doc_no: str, timeout: int = 20
) -> str | None:
    sc_url = urljoin(
        kind_base_url, f"/common/disclsviewer.do?method=searchContents&docNo={doc_no}"
    )
    text = _get(sc_url, timeout=timeout, hint=kind_base_url)

    # setPath('tocLocPath','docLocPath','server',...)
    m = re.search(r"setPath\(\s*['\"][^'\"]*['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,", text)
    if m:
        return m.group(1).strip()
    # 백업: setPath2(...)
    m2 = re.search(
        r"setPath2\(\s*['\"][^'\"]*['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,", text
    )
    return m2.group(1).strip() if m2 else None


def _kind_fetch_text_from_docloc(
    kind_base_url: str, doc_loc_path: str, timeout: int = 20
) -> str:
    doc_url = urljoin(kind_base_url, doc_loc_path)
    html = _get(doc_url, timeout=timeout, hint=kind_base_url)
    return _strip_tags(html)


# -------------------------
# iframe 백업 로직 (DART/KIND 공통)
# -------------------------
def _find_iframe_src(viewer_html: str) -> str | None:
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


def _fetch_iframe_text(viewer_html: str, viewer_url: str, timeout: int = 20) -> str:
    src = _find_iframe_src(viewer_html)
    if not src:
        return ""
    iframe_url = urljoin(viewer_url, src)
    html = _get(iframe_url, timeout=timeout, hint=viewer_url)
    return _strip_tags(html)


# -------------------------
# 최종 진입점
# -------------------------
def get_disclosure_supply(url: str, timeout: int = 20) -> str:
    viewer_html = fetch_viewer_html(url, timeout=timeout)

    if "kind.krx.co.kr" in url.lower():
        acpt, doc = _kind_pick_acpt_doc(viewer_html)
        if doc:
            doc_loc = _kind_fetch_doclocpath(url, doc, timeout=timeout)
            if doc_loc:
                text = _kind_fetch_text_from_docloc(url, doc_loc, timeout=timeout)
                if text and len(text) > 50:
                    return text
        return _fetch_iframe_text(viewer_html, url, timeout=timeout)

    # DART 등은 기본적으로 iframe 따라가기 (DART도 EUC-KR 페이지가 있음 → _get 사용)
    return _fetch_iframe_text(viewer_html, url, timeout=timeout)
