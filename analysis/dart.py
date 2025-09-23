import os
from typing import Any, Dict, Optional, Union

import requests
from dotenv import load_dotenv

load_dotenv()

DART_BASE_URL = "https://opendart.fss.or.kr/api"
DART_API_KEY = os.getenv("DART_API_KEY")


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
    endpoint: str,
    corp_code: Optional[Union[str, int]] = None,
    bgn_de: Optional[Union[str, int]] = None,
    end_de: Optional[Union[str, int]] = None,
) -> Dict[str, Any]:
    """
    DART API 공통 호출 함수.
    - endpoint: 'cvbdIsDecsn' 또는 'otcprStkInvscrInhDecsn' 같은 엔드포인트 이름
                혹은 전체 URL('https://...json')도 허용
    - corp_code: 8자리 고유번호(숫자만 허용, zero-pad 자동)
    - bgn_de, end_de: YYYYMMDD 문자열(형식 자동 정규화)
    - extra_params: 추가 쿼리 파라미터

    반환: DART API 응답 JSON (dict). HTTP/네트워크 오류도 dict로 통일하여 반환.
    """
    if not DART_API_KEY:
        return {
            "status": "ENV",
            "message": "DART_API_KEY 환경변수가 설정되지 않았습니다.",
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
    전환사채권 발행결정 조회 (cvbdIsDecsn)
    - corp_code: 8자리 고유번호
    - bgn_de, end_de: YYYYMMDD
    """
    return send_dart_api(
        "cvbdIsDecsn",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )


def get_stock_acquisition(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> Dict[str, Any]:
    """
    타법인 주식 및 출자증권 양수결정 조회 (otcprStkInvscrInhDecsn)
    - corp_code: 8자리 고유번호
    - bgn_de, end_de: YYYYMMDD
    """
    return send_dart_api(
        "otcprStkInvscrInhDecsn",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )
