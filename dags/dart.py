import os
import random
import time
from typing import Any, Dict, List, Optional, Union

import requests
from dotenv import load_dotenv

from dart_schemas import DISCLOSURE_SCHEMAS

load_dotenv()

DART_BASE_URL = "https://opendart.fss.or.kr/api"
DART_API_KEY = os.getenv("DART_API_KEY")

SESSION = requests.Session()

# 요청 간 기본 딜레이(정중함) + 지터 범위
_BASE_DELAY_SEC = 0.1  # 최소 대기
_JITTER_SEC = 0.15  # 0 ~ 0.25초 랜덤 추가


def _polite_pause():
    """요청 사이에 기본 딜레이 + 지터."""
    time.sleep(_BASE_DELAY_SEC + random.random() * _JITTER_SEC)


class DartAPIError(Exception):
    """DART API 호출 실패 예외"""

    pass


def _normalize_date(yyyymmdd: Optional[Union[str, int]]) -> Optional[str]:
    if yyyymmdd is None:
        return None
    s = str(yyyymmdd).strip()
    # 숫자만 추출
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) != 8:
        return None
    return digits


def _format_amount(amount_str: Optional[str]) -> str:
    """금액을 '9,999원 (99.9억)' 형식으로 변환"""
    if not amount_str or amount_str == "-":
        return "-"
    try:
        amount = int(amount_str)
        return f"{amount:,}원 ({amount/100000000:.1f}억)"
    except:
        return amount_str


def _format_percent(percent_str: Optional[str]) -> str:
    """퍼센트 값에 % 기호 추가"""
    if not percent_str or percent_str == "-":
        return "-"
    return f"{percent_str}%"


def _format_shares(shares_str: Optional[str]) -> str:
    """주식 수를 '9,999주' 형식으로 변환"""
    if not shares_str or shares_str == "-":
        return "-"
    try:
        shares = int(shares_str)
        return f"{shares:,}주"
    except:
        return shares_str


def _apply_format(value: Optional[str], format_type: Optional[str] = None) -> str:
    """값에 포맷을 적용"""
    if format_type == "amount":
        return _format_amount(value)
    elif format_type == "percent":
        return _format_percent(value)
    elif format_type == "shares":
        return _format_shares(value)
    else:
        return value if value else "-"


def _get_field_value(
    data: Dict[str, Any],
    key: Union[str, List[str]],
    format_type: Union[str, List[str], None] = None,
    template: Optional[str] = None,
) -> str:
    """
    스키마에서 정의한 키로 값을 가져와서 포맷팅
    - key: 단일 키 또는 키 리스트 (템플릿용)
    - format_type: 단일 포맷 또는 포맷 리스트
    - template: 복수 값을 조합할 때 사용 (예: "{} ~ {}")
    """
    # 단일 키인 경우
    if isinstance(key, str):
        value = data.get(key)
        return _apply_format(value, format_type)

    # 복수 키인 경우 (template 사용)
    elif isinstance(key, list):
        values = []
        formats = (
            format_type if isinstance(format_type, list) else [format_type] * len(key)
        )

        for k, fmt in zip(key, formats):
            val = data.get(k)
            formatted = _apply_format(val, fmt)
            values.append(formatted)

        if template:
            return template.format(*values)
        else:
            return ", ".join(values)

    return "-"


def _check_field_has_value(data: Dict[str, Any], field_config: Dict[str, Any]) -> bool:
    """필드에 의미있는 값이 있는지 확인 (optional 필드 체크용)"""
    key = field_config.get("key")

    if isinstance(key, str):
        value = data.get(key)
        return value is not None and value != "" and value != "-"
    elif isinstance(key, list):
        # 복수 키 중 하나라도 값이 있으면 True
        return any(
            data.get(k) is not None and data.get(k) != "" and data.get(k) != "-"
            for k in key
        )
    return False


def _validate_data_not_empty(data: Dict[str, Any], excluded_fields: set) -> None:
    """
    데이터가 excluded_fields를 제외하고 모두 비어있는지 확인
    비어있으면 DartAPIError 발생
    """
    all_empty = True
    for key, value in data.items():
        if key not in excluded_fields:
            if value is not None and value != "" and value != "-":
                all_empty = False
                break

    if all_empty:
        raise DartAPIError("필드가 누락되어있습니다")


def render_disclosure_from_schema(disclosure_type: str, data: Dict[str, Any]) -> str:
    """
    스키마 기반으로 공시 정보를 텍스트로 렌더링

    Args:
        disclosure_type: 공시 유형 (예: "유상증자 결정")
        data: DART API 응답 데이터

    Returns:
        포맷팅된 텍스트 문자열

    Raises:
        DartAPIError: 데이터가 없거나 모든 필드가 비어있을 때
    """
    # API 응답 검증
    if not data or data.get("status") != "000" or not data.get("list"):
        raise DartAPIError("데이터가 없거나 API 요청 오류입니다.")

    info = data["list"][0]

    # 스키마 가져오기
    schema = DISCLOSURE_SCHEMAS.get(disclosure_type)
    if not schema:
        raise DartAPIError(f"'{disclosure_type}'에 대한 스키마가 정의되지 않았습니다.")

    # 데이터 검증 (모든 필드가 비어있지 않은지 확인)
    excluded_fields = schema.get("excluded_fields", set())
    _validate_data_not_empty(info, excluded_fields)

    # 렌더링 시작
    lines = []

    for section in schema.get("sections", []):
        # 섹션 제목 추가
        section_title = section.get("title")
        if section_title:
            lines.append(section_title)
            if section.get("separator"):
                lines.append("=" * 20)

        # 서브섹션이 있는 경우 (예: 자기주식 보유현황)
        if "subsections" in section:
            for subsection in section["subsections"]:
                if subsection.get("title"):
                    lines.append(subsection["title"])

                for field in subsection.get("fields", []):
                    label = field.get("label")
                    value = _get_field_value(
                        info,
                        field.get("key"),
                        field.get("format"),
                        field.get("template"),
                    )

                    if label:
                        lines.append(f"{label}: {value}")
                    else:
                        lines.append(value)

        # 일반 필드 처리
        else:
            section_optional = section.get("optional", False)
            has_any_value = False
            section_lines = []

            for field in section.get("fields", []):
                field_optional = field.get("optional", False)

                # optional 필드는 값이 있을 때만 출력
                if field_optional and not _check_field_has_value(info, field):
                    continue

                label = field.get("label")
                value = _get_field_value(
                    info, field.get("key"), field.get("format"), field.get("template")
                )

                # suffix 처리
                if field.get("suffix"):
                    value = value + field.get("suffix")

                # 값이 있으면 섹션에 포함
                if value and value != "-":
                    has_any_value = True

                if label:
                    section_lines.append(f"{label}: {value}")
                else:
                    # 라벨 없이 값만 출력
                    section_lines.append(value)

            # optional 섹션은 값이 하나라도 있을 때만 추가
            if not section_optional or has_any_value:
                lines.extend(section_lines)

        # 섹션 사이 공백 추가
        lines.append("")

    # 마지막 빈 줄 제거
    while lines and lines[-1] == "":
        lines.pop()

    return "\n".join(lines)


def send_dart_api(
    disclosure_type: str,
    corp_code: Optional[Union[str, int]] = None,
    bgn_de: Optional[Union[str, int]] = None,
    end_de: Optional[Union[str, int]] = None,
) -> Dict[str, Any]:
    """
    DART API 공통 호출 함수.
    - disclosure_type: config.py의 keywords 키 (예: "전환사채권 발행결정")
    - corp_code: 8자리 고유번호(숫자만 허용, zero-pad 자동)
    - bgn_de, end_de: YYYYMMDD 문자열(형식 자동 정규화)

    반환: DART API 응답 JSON (dict). HTTP/네트워크 오류도 dict로 통일하여 반환.
    """
    if not DART_API_KEY:
        return {
            "status": "ENV",
            "message": "DART_API_KEY 환경변수가 설정되지 않았습니다.",
        }

    # dart_API_map에서 endpoint 찾기
    endpoint = dart_API_map.get(disclosure_type)
    if endpoint is None:
        return {
            "status": "NO_API",
            "message": f"'{disclosure_type}'에 해당하는 DART API가 없습니다.",
        }

    ep = endpoint if endpoint.endswith(".json") else f"{endpoint}.json"
    url = f"{DART_BASE_URL}/{ep}"

    params: Dict[str, Any] = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": _normalize_date(bgn_de),
        "end_de": _normalize_date(end_de),
    }

    try:
        _polite_pause()  # API 요청 전 지터 추가
        resp = SESSION.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        # DART 표준 상태코드 처리
        status = data.get("status")
        # '000' 정상
        if status == "000":
            return data

        # 비정상 응답도 그대로 반환
        return data

    except requests.exceptions.RequestException as e:
        return {"status": "HTTP_ERROR", "message": f"HTTP 오류: {e}"}
    except ValueError:
        return {
            "status": "PARSE_ERROR",
            "message": "응답을 JSON으로 파싱할 수 없습니다.",
        }
    except Exception as e:
        return {"status": "UNKNOWN_ERROR", "message": f"알 수 없는 오류: {e}"}


def get_disclosure(
    disclosure_type: str,
    corp_code: Union[str, int],
    date: Union[str, int],
) -> str:
    """
    공시 정보 조회 공통 함수 (스키마 기반)

    Args:
        disclosure_type: 공시 유형 (예: "유상증자 결정", "전환사채권 발행결정")
        corp_code: 8자리 고유번호
        date: YYYYMMDD

    Returns:
        포맷팅된 공시 텍스트

    Raises:
        DartAPIError: API 호출 실패 또는 데이터 없음
    """
    data = send_dart_api(disclosure_type, corp_code=corp_code, bgn_de=date, end_de=date)
    return render_disclosure_from_schema(disclosure_type, data)


# config.py의 keywords 키와 DART API 명칭 매핑
dart_API_map = {
    "임상 계획 철회": None,  # DART API 없음
    "임상 계획 신청": None,  # DART API 없음
    "임상 계획 승인": None,  # DART API 없음
    "임상 계획 결과 발표": None,  # DART API 없음
    "자산양수도(기타), 풋백옵션": "astInhtrfEtcPtbkOpt",  # DART API 없음
    "부도발생": None,  # DART API 없음
    "영업정지": "bsnSp",
    "회생절차 개시신청": "ctrcvsBgrq",
    "해산사유 발생": "dsRsOcr",  # DART API 없음
    "유상증자 결정": "piicDecsn",  # DART API 없음
    "무상증자 결정": "fricDecsn",  # DART API 없음
    "유무상증자 결정": None,  # DART API 없음
    "감자 결정": "crDecsn",  # DART API 없음
    "채권은행 등의 관리절차 개시": "bnkMngtPcbg",  # DART API 없음
    "소송 등의 제기": "lwstLg",  # TODO: 성공 30 실패 311
    "해외 증권시장 주권등 상장 결정": "ovLstDecsn",  # DART API 없음
    "해외 증권시장 주권등 상장폐지 결정": "ovDlstDecsn",  # DART API 없음
    "해외 증권시장 주권등 상장": "ovLst",  # DART API 없음
    "해외 증권시장 주권등 상장폐지": "ovDlst",  # DART API 없음
    "전환사채권 발행결정": "cvbdIsDecsn",
    "신주인수권부사채권 발행결정": "bdwtIsDecsn",  # DART API 없음
    "교환사채권 발행결정": "exbdIsDecsn",
    "채권은행 등의 관리절차 중단": "bnkMngtPcsp",  # DART API 없음
    "상각형 조건부자본증권 발행결정": "wdCocobdIsDecsn",  # DART API 없음
    "자기주식 취득 결정": "tsstkAqDecsn",
    "자기주식 처분 결정": "tsstkDpDecsn",
    "자기주식 소각 결정": None,  # DART API 없음
    "자기주식취득 신탁계약 체결 결정": "tsstkAqTrctrCnsDecsn",
    "자기주식취득 신탁계약 해지 결정": "tsstkAqTrctrCcDecsn",  # DART API 없음
    "영업양수 결정": "bsnInhDecsn",  # DART API 없음
    "영업양도 결정": "bsnTrfDecsn",  # DART API 없음
    "유형자산 양수 결정": "tgastInhDecsn",  # DART API 없음
    "유형자산 양도 결정": "tgastTrfDecsn",  # DART API 없음
    "타법인 주식 및 출자증권 양수결정": "otcprStkInvscrInhDecsn",
    "타법인 주식 및 출자증권 양도결정": "otcprStkInvscrTrfDecsn",
    "주권 관련 사채권 양수 결정": "stkrtbdInhDecsn",  # DART API 없음
    "주권 관련 사채권 양도 결정": "stkrtbdTrfDecsn",  # DART API 없음
    "회사합병 결정": "cmpMgDecsn",  # DART API 없음
    "회사분할 결정": "cmpDvDecsn",  # DART API 없음
    "회사분할합병 결정": "cmpDvmgDecsn",  # DART API 없음
    "주식교환·이전 결정": "stkExtrDecsn",  # DART API 없음
    "지분공시": None,  # DART API 없음
    "실적공시": None,  # DART API 없음
    "단일판매ㆍ공급계약해지": None,  # DART API 없음
    "단일판매ㆍ공급계약체결": None,  # DART API 없음
    "생산중단": None,  # DART API 없음
    "배당": None,  # DART API 없음
    "매출액변동": None,  # DART API 없음
    "소송등의판결ㆍ결정": None,  # DART API 없음
    "특허권취득": None,  # DART API 없음
    "신규시설투자": None,  # DART API 없음
    "기술이전계약해지": None,  # DART API 없음
    "기술이전계약체결": None,  # DART API 없음
    "품목허가 철회": None,  # DART API 없음
    "품목허가 신청": None,  # DART API 없음
    "품목허가 승인": None,  # DART API 없음
    "횡령ㆍ배임혐의발생": None,  # DART API 없음
    "공개매수": None,  # DART API 없음
}
