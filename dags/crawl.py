from typing import Any, Dict, Optional, Union
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from disclosure_common import (
    normalize_cell_text,
)

load_dotenv()


def format_table_supply_contract(table: BeautifulSoup) -> str:
    """유상증자 등 공시에 맞게 tr/td 구조를 텍스트로 정제한다."""
    rows = table.find_all("tr")
    output_lines = []

    for tr in rows:
        # td/th 모두 허용(간혹 th가 들어오는 공시가 있음)
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        # 각 셀 텍스트 정규화
        tds = [normalize_cell_text(td) for td in cells]

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


def format_table_performance_change(table: BeautifulSoup) -> str:
    """경영실적 변동에 맞게 tr/td 구조를 텍스트로 정제한다."""
    rows = table.find_all("tr")
    result = []
    # print(rows)

    for row in rows:
        cols = [col.get_text(strip=True) for col in row.find_all("td")]
        if not cols:
            continue

        # 1. 단일 항목 (예: "1. 재무제표의 종류")
        if len(cols) == 2 and cols[0].startswith("1."):
            result.append(f"{cols[0]} : {cols[1]}")
        # 2. 매출액 또는 손익구조변동내용
        elif len(cols) == 5 and cols[0].startswith("2."):
            result.append(f"\n{cols[0]}")
        elif (
            len(cols) == 5
            and cols[0].startswith("- ")
            and ("매출액" in cols[0] or "영업이익" in cols[0])
        ):
            # 세부 항목 (매출액, 영업이익 등)
            item = cols[0].replace("- ", "")
            result.append(f"\n- {item}")
            result.append(f"당해사업연도: {cols[1]}")
            result.append(f"직전사업연도: {cols[2]}")
            result.append(f"증감금액: {cols[3]}")
            result.append(f"증감비율(%): {cols[4]}")
        elif cols[0].startswith("4."):
            tds = row.find_all("td")
            head = tds[0].get_text(strip=True)
            body = tds[1].get_text(separator="\n", strip=True)  # <--- 핵심

            result.append(f"\n{head}")
            result.append(body)

    return "\n".join(result)


# config.py의 keywords 키와 DART API 명칭 매핑
crawling_function_map = {
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
    "전환사채권 발행결정": None,
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
    "단일판매ㆍ공급계약해지": format_table_supply_contract,
    "단일판매ㆍ공급계약체결": None,
    "생산중단": None,
    "배당": None,
    "매출액변동": format_table_performance_change,
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
