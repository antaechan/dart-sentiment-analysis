import os
import random
import time
from typing import Any, Dict, Optional, Union

import requests
from dotenv import load_dotenv

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


def get_convertible_bond(
    corp_code: Union[str, int],
    date: Union[str, int],
) -> str:
    """
    전환사채권 발행결정 조회
    - corp_code: 8자리 고유번호
    - date: YYYYMMDD
    """
    data = send_dart_api(
        "전환사채권 발행결정",
        corp_code=corp_code,
        bgn_de=date,
        end_de=date,
    )

    lines = []
    if not data or data.get("status") != "000" or not data.get("list"):
        raise DartAPIError("데이터가 없거나 API 요청 오류입니다.")

    bond_info = data["list"][0]

    # 숫자, 퍼센트 변환 함수 모두 내부로 이동
    def format_amount(amount_str):
        if not amount_str or amount_str == "-":
            return "-"
        try:
            amount = int(amount_str)
            return f"{amount:,}원 ({amount/100000000:.1f}억)"
        except:
            return amount_str

    def format_percent(percent_str):
        if not percent_str or percent_str == "-":
            return "-"
        return f"{percent_str}%"

    lines.append(f"전환사채 발행정보")
    lines.append("=" * 50)
    lines.append(f"회사명: {bond_info.get('corp_name', '-')}")
    lines.append(f"접수번호: {bond_info.get('rcept_no', '-')}")
    lines.append(f"이사회결의일: {bond_info.get('bddd', '-')}")
    lines.append("")

    lines.append(f"발행정보")
    lines.append(f"사채종류: {bond_info.get('bd_knd', '-')}")
    lines.append(f"발행금액: {format_amount(bond_info.get('bd_fta'))}")
    lines.append(f"발행방법: {bond_info.get('bdis_mthn', '-')}")
    lines.append(f"사채만기일: {bond_info.get('bd_mtd', '-')}")
    lines.append("")

    lines.append(f"이자율 정보")
    lines.append(f"표면이자율: {format_percent(bond_info.get('bd_intr_ex'))}")
    lines.append(f"만기이자율: {format_percent(bond_info.get('bd_intr_sf'))}")
    lines.append("")

    lines.append(f"전환 정보")
    lines.append(f"전환가액: {format_amount(bond_info.get('cv_prc'))} (1주당)")
    lines.append(f"전환비율: {format_percent(bond_info.get('cv_rt'))}")
    lines.append(f"주식총수 대비: {format_percent(bond_info.get('cvisstk_tisstk_vs'))}")
    lines.append(
        f"전환청구기간: {bond_info.get('cvrqpd_bgd', '-')} ~ {bond_info.get('cvrqpd_edd', '-')}"
    )
    lines.append(
        f"최저조정가액: {format_amount(bond_info.get('act_mktprcfl_cvprc_lwtrsprc'))}"
    )
    lines.append("")

    lines.append(f"일정")
    lines.append(f"청약일: {bond_info.get('sbd', '-')}")
    lines.append(f"납입일: {bond_info.get('pymd', '-')}")
    lines.append("")

    # 자금조달 목적이 있는 경우만 출력
    funding_purposes = []
    if bond_info.get("fdpp_fclt") not in [None, "-", ""]:
        funding_purposes.append(
            f"시설자금: {format_amount(bond_info.get('fdpp_fclt'))}"
        )
    if bond_info.get("fdpp_op") not in [None, "-", ""]:
        funding_purposes.append(f"운영자금: {format_amount(bond_info.get('fdpp_op'))}")
    if bond_info.get("fdpp_dtrp") not in [None, "-", ""]:
        funding_purposes.append(
            f"채무상환자금: {format_amount(bond_info.get('fdpp_dtrp'))}"
        )
    if bond_info.get("fdpp_ocsa") not in [None, "-", ""]:
        funding_purposes.append(
            f"타법인증권취득자금: {format_amount(bond_info.get('fdpp_ocsa'))}"
        )
    if bond_info.get("fdpp_etc") not in [None, "-", ""]:
        funding_purposes.append(f"기타자금: {format_amount(bond_info.get('fdpp_etc'))}")

    if funding_purposes:
        lines.append(f"자금조달 목적")
        for purpose in funding_purposes:
            lines.append(f"{purpose}")
        lines.append("")

    lines.append(f"기타정보")
    if bond_info.get("rpmcmp") not in [None, "-", ""]:
        lines.append(f"대표주관회사: {bond_info.get('rpmcmp', '-')}")
    if bond_info.get("grint") not in [None, "-", ""]:
        lines.append(f"보증기관: {bond_info.get('grint', '-')}")
    lines.append(f"증권신고서 제출대상: {bond_info.get('rs_sm_atn', '-')}")
    if bond_info.get("ex_sm_r") not in [None, "-", ""]:
        lines.append(f"제출면제사유: {bond_info.get('ex_sm_r', '-')}")
    return "\n".join(lines)


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

# config.py의 keywords 키와 DART API 명칭 매핑
dart_API_function_map = {
    "임상 계획 철회": None,
    "임상 계획 신청": None,
    "임상 계획 승인": None,
    "임상 계획 결과 발표": None,
    "자산양수도(기타), 풋백옵션": None,
    "부도발생": None,
    "영업정지": None,
    "회생절차 개시신청": None,
    "해산사유 발생": None,
    "유상증자 결정": None,
    "무상증자 결정": None,
    "유무상증자 결정": None,
    "감자 결정": None,
    "채권은행 등의 관리절차 개시": None,
    "소송 등의 제기": None,
    "해외 증권시장 주권등 상장 결정": None,
    "해외 증권시장 주권등 상장폐지 결정": None,
    "해외 증권시장 주권등 상장": None,
    "해외 증권시장 주권등 상장폐지": None,
    "전환사채권 발행결정": get_convertible_bond,
    "신주인수권부사채권 발행결정": None,
    "교환사채권 발행결정": None,
    "채권은행 등의 관리절차 중단": None,
    "상각형 조건부자본증권 발행결정": None,
    "자기주식 취득 결정": None,
    "자기주식 처분 결정": None,
    "자기주식 소각 결정": None,
    "자기주식취득 신탁계약 체결 결정": None,
    "자기주식취득 신탁계약 해지 결정": None,
    "영업양수 결정": None,
    "영업양도 결정": None,
    "유형자산 양수 결정": None,
    "유형자산 양도 결정": None,
    "타법인 주식 및 출자증권 양수결정": None,
    "타법인 주식 및 출자증권 양도결정": None,
    "주권 관련 사채권 양수 결정": None,
    "주권 관련 사채권 양도 결정": None,
    "회사합병 결정": None,
    "회사분할 결정": None,
    "회사분할합병 결정": None,
    "주식교환·이전 결정": None,
    "지분공시": None,
    "실적공시": None,
    "단일판매ㆍ공급계약해지": None,
    "단일판매ㆍ공급계약체결": None,
    "생산중단": None,
    "배당": None,
    "매출액변동": None,
    "소송등의판결ㆍ결정": None,
    "특허권취득": None,
    "신규시설투자": None,
    "기술이전계약해지": None,
    "기술이전계약체결": None,
    "품목허가 철회": None,
    "품목허가 신청": None,
    "품목허가 승인": None,
    "횡령ㆍ배임혐의발생": None,
    "공개매수": None,
}
