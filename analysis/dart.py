import os
from typing import Any, Dict, Optional, Union

import requests
from dotenv import load_dotenv

load_dotenv()

DART_BASE_URL = "https://opendart.fss.or.kr/api"
DART_API_KEY = os.getenv("DART_API_KEY")


# config.py의 keywords 키와 DART API 명칭 매핑
dart_API_map = {
    "임상 계획 철회": None,  # DART API 없음
    "임상 계획 신청": None,  # DART API 없음
    "임상 계획 승인": None,  # DART API 없음
    "임상 계획 결과 발표": None,  # DART API 없음
    "자산양수도(기타), 풋백옵션": "astInhtrfEtcPtbkOpt",  # DART API 없음
    "부도발생": None,  # DART API 없음
    "영업정지": None,  # DART API 없음
    "회생절차 개시신청": "ctrcvsBgrq",  # DART API 없음
    "해산사유 발생": "ctrcvsBgrq",  # DART API 없음
    "유상증자 결정": "ctrcvsBgrq",  # DART API 없음
    "무상증자 결정": "fricDecsn",  # DART API 없음
    "유무상증자 결정": None,  # DART API 없음
    "감자 결정": "crDecsn",  # DART API 없음
    "채권은행 등의 관리절차 개시": "bnkMngtPcbg",  # DART API 없음
    "소송 등의 제기": "lwstLg",  # DART API 없음
    "해외 증권시장 주권등 상장 결정": "ovLstDecsn",  # DART API 없음
    "해외 증권시장 주권등 상장폐지 결정": "ovDlstDecsn",  # DART API 없음
    "해외 증권시장 주권등 상장": "ovLst",  # DART API 없음
    "해외 증권시장 주권등 상장폐지": "ovDlst",  # DART API 없음
    "전환사채권 발행결정": "cvbdIsDecsn",
    "신주인수권부사채권 발행결정": "bdwtIsDecsn",  # DART API 없음
    "교환사채권 발행결정": "exbdIsDecsn",  # DART API 없음
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


class DartAPIError(Exception):
    """DART API 호출 실패 예외"""

    pass


def _normalize_corp_code(corp_code: Optional[Union[str, int]]) -> Optional[str]:
    if corp_code is None:
        return None
    code = str(corp_code).strip()
    if not code:
        return None
    # 숫자만 남기고 8자리 zero-pad
    digits = "".join(ch for ch in code if ch.isdigit())
    if not digits:
        return None
    return digits.zfill(8)


def _normalize_date(yyyymmdd: Optional[Union[str, int]]) -> Optional[str]:
    if yyyymmdd is None:
        return None
    s = str(yyyymmdd).strip()
    # 숫자만 추출
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) != 8:
        return None
    return digits


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
        "corp_code": _normalize_corp_code(corp_code),
        "bgn_de": _normalize_date(bgn_de),
        "end_de": _normalize_date(end_de),
    }

    try:
        resp = requests.get(url, params=params)
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


def get_convertible_bond(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    전환사채권 발행결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    return send_dart_api(
        "전환사채권 발행결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )


def get_stock_acquisition(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    타법인 주식 및 출자증권 양수결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    return send_dart_api(
        "타법인 주식 및 출자증권 양수결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )


def get_stock_sale(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    타법인 주식 및 출자증권 양도결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    return send_dart_api(
        "타법인 주식 및 출자증권 양도결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )


def get_treasury_stock_trust(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    자기주식취득 신탁계약 체결 결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    return send_dart_api(
        "자기주식취득 신탁계약 체결 결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )


def get_treasury_stock_buy(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    자기주식 취득 결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    return send_dart_api(
        "자기주식 취득 결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )


def get_treasury_stock_sell(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    자기주식 처분 결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    return send_dart_api(
        "자기주식 처분 결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )
