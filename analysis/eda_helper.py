#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공시 유형별 주가 영향 분석
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import warnings
import re

warnings.filterwarnings("ignore")

# 한글 폰트 설정
plt.rcParams["font.family"] = "DejaVu Sans"
plt.rcParams["axes.unicode_minus"] = False


def load_data():
    """데이터 로드"""
    try:
        # CSV 파일 로드
        df = pd.read_csv("~/Downloads/disclosure_events_sql.csv")
        print(f"데이터 로드 완료: {len(df)}건")
        return df
    except FileNotFoundError:
        print("CSV 파일을 찾을 수 없습니다. 경로를 확인해주세요.")
        return None


def basic_analysis(df):
    """기본 데이터 분석"""
    print("\n=== 기본 데이터 분석 ===")

    # 컬럼 정보
    print(f"컬럼 수: {len(df.columns)}")
    print(f"컬럼명: {list(df.columns)}")

    # 데이터 타입
    print("\n데이터 타입:")
    print(df.dtypes)

    # 결측값 확인
    print("\n결측값 현황:")
    print(df.isnull().sum())

    # 수익률 컬럼 확인
    ret_columns = ["ret_1m", "ret_3m", "ret_10m", "ret_60m"]
    print(f"\n수익률 컬럼: {ret_columns}")

    return ret_columns


def classify_disclosure_type(report_name):
    """
    키워드 기반으로 공시 유형을 분류하는 함수

    Args:
        report_name (str): 공시명

    Returns:
        str: 분류된 공시 유형
    """
    if pd.isna(report_name) or report_name == "":
        return "기타"

    report_name = str(report_name).lower()

    # 주요 공시 유형별 키워드 정의 (DART API 기반)
    disclosure_keywords = {
        "자산양수도": [
            "자산양수도",
            "자산양수도(기타)",
            "풋백옵션",
            "유형자산 양수",
            "유형자산 양도",
            "타법인 주식 및 출자증권 양수",
            "타법인 주식 및 출자증권 양도",
            "주권 관련 사채권 양수",
            "주권 관련 사채권 양도",
            "타법인주식및출자증권양수결정",
            "타법인주식및출자증권취득결정",
            "타법인주식및출자증권처분결정",
            "유형자산양수결정",
        ],
        "부도발생": [
            "부도발생",
            "부도",
            "파산",
            "도산",
        ],
        "영업정지": [
            "영업정지",
            "영업중단",
            "영업활동 중단",
            "생산중단",
        ],
        "회생절차": [
            "회생절차",
            "회생절차 개시신청",
            "회생",
            "법정관리",
        ],
        "해산": [
            "해산",
            "해산사유 발생",
            "회사해산",
            "청산",
        ],
        "유무상증자": [
            "유상증자",
            "유상증자 결정",
            "신주발행",
            "증자",
            "신주",
            "무상증자",
            "무상증자 결정",
            "무상주",
            "유무상증자",
            "유무상증자 결정",
        ],
        "감자": [
            "감자",
            "감자 결정",
            "자본감소",
            "주식감소",
        ],
        "채권은행관리": [
            "채권은행",
            "채권은행 등의 관리절차",
            "관리절차 개시",
            "관리절차 중단",
        ],
        "소송": ["소송", "소송 등의 제기", "법적분쟁", "고소", "고발", "법률"],
        "해외상장": [
            "해외 증권시장",
            "해외 상장",
            "해외 상장 결정",
            "해외 상장폐지",
            "해외 상장폐지 결정",
        ],
        "신주인수권부사채권": [
            "신주인수권부사채권",
            "신주인수권부사채권 발행결정",
            "신주인수권",
        ],
        "교환사채권": [
            "교환사채권",
            "교환사채권 발행결정",
            "교환사채",
        ],
        "상각형조건부자본증권": [
            "상각형 조건부자본증권",
            "상각형 조건부자본증권 발행결정",
            "AT1",
            "조건부자본증권",
        ],
        "자사주취득": [
            "자기주식 취득",
            "자기주식취득",
            "자기주식취득 신탁계약",
            "자사주 취득",
            "자기주식 매입",
        ],
        "자사주처분": [
            "자기주식 처분",
            "자기주식처분",
            "자기주식 매각",
            "자사주 처분",
        ],
        "자사주소각": [
            "주식소각",
            "주식소각결정",
            "소각",
        ],
        "전환사채권발행": [
            "전환사채권",
            "전환사채권발행결정",
            "전환사채",
        ],
        "영업양수도": [
            "영업양수",
            "영업양도",
            "영업양수 결정",
            "영업양도 결정",
        ],
        "회사합병": [
            "회사합병",
            "회사합병 결정",
            "합병",
            "기업합병",
        ],
        "회사분할": [
            "회사분할",
            "회사분할 결정",
            "회사분할합병",
        ],
        "주식교환이전": [
            "주식교환",
            "주식이전",
            "주식교환·이전",
            "주식교환·이전 결정",
        ],
        "공급계약체결": [
            "단일판매ㆍ공급계약체결",
            "계약체결",
            "공급계약",
            "판매계약",
            "공급결정",
            "수주",
            "수주계약",
        ],
        "IR활동": [
            "기업설명회",
            "IR개최",
            "투자자관계",
            "IR활동",
        ],
        "실적공시": [
            "영업실적",
            "잠정실적",
            "연결재무제표",
            "실적공시",
            "영업(잠정)실적",
            "분기보고서",
            "반기보고서",
            "사업보고서",
        ],
        "특허권": [
            "특허권 취득",
            "특허",
            "지적재산권",
            "라이선스",
        ],
        "기술이전": [
            "기술이전",
            "기술라이선스",
            "기술계약",
            "마일스톤",
        ],
        "품목허가승인": [
            "품목허가 승인",
            "품목허가 취득",
            "허가취득",
            "허가 획득",
            "승인 취득",
        ],
        "품목허가신청": [
            "허가신청",
            "품목허가 신청",
            "품목 허가 신청",
            "품목허가 변경 신청",
        ],
        "임상시험계획신청": [
            "임상시험계획 신청",
            "임상시험계획승인신청",
            "승인신청",
            "시험계획서(CTN) 제출",
        ],
        "임상시험계획승인": [
            "임상시험계획 승인",
            "임상시험계획 변경 승인",
            "임상시험계획(IND) 승인)",
        ],
        "전환가액조정": [
            "전환가액의조정",
            "전환가액 조정",
            "전환가액",
        ],
        "지분공시": [
            "최대주주변경",
            "최대주주등소유주식변동신고서",
            "주주변경",
            "주식변동",
            "주식등의대량보유상황보고서",
            "임원ㆍ주요주주특정증권등소유상황보고서",
        ],
        "신규투자": [
            "신규시설투자",
            "시설투자",
            "투자결정",
            "투자계획",
        ],
        "조회공시": [
            "조회공시요구",
            "조회공시",
            "답변",
            "현저한시황변동",
            "풍문또는보도",
        ],
        "수시공시": [
            "수시공시의무관련사항",
            "수시공시",
            "공시의무",
        ],
        "장래계획": [
            "장래사업ㆍ경영계획",
            "경영계획",
            "사업계획",
            "미래계획",
        ],
        "매출변동": [
            "매출액또는손익구조",
            "매출변동",
            "손익구조",
            "매출구조",
        ],
        "전환청구권행사": [
            "전환청구권행사",
            "전환청구",
            "전환행사",
        ],
        "자문용역": [
            "자문용역",
            "자문계약",
            "용역계약",
        ],
        "국책과제": [
            "국책과제",
            "정부과제",
            "과제선정",
        ],
        "해외진출": [
            "해외진출",
            "해외시장",
            "해외공급",
            "수출",
        ],
        "자회사경영": [
            "자회사의주요경영사항",
            "자회사경영",
            "종속회사",
        ],
        "자율공시": [
            "자율공시",
            "자율",
        ],
        "공정공시": [
            "공정공시",
            "공정",
        ],
        "안내공시": [
            "안내공시",
            "안내",
        ],
        # 추가 카테고리
        "배당": [
            "현금ㆍ현물배당결정",
            "현금배당",
            "현물배당",
            "주식배당",
            "배당결정",
            "분기배당",
        ],
        "투자판단": [
            "투자판단관련주요경영사항",
            "투자판단",
            "주요경영사항",
        ],
    }

    if "기술이전" in report_name:
        return "기술이전"

    if "국책과제" in report_name:
        return "국책과제"

    # 임상 관련 세부 분류
    if "임상" in report_name:
        if "임상시험계획승인신청등결정" in report_name:
            return "임상시험계획승인"

        # 임상시험계획신청 (임상, 신청 모두 포함)
        if "임상" in report_name and (
            "신청" in report_name
            or "제출" in report_name
            or "시험계획서" in report_name
        ):
            return "임상시험계획신청"
        # 임상시험계획승인 (임상, 계획, 승인 모두 포함)
        elif all(keyword in report_name for keyword in ["임상", "계획", "승인"]):
            return "임상시험계획승인"
        # 임상시험결과 (임상, 결과 모두 포함)
        if "임상" in report_name and ("결과" in report_name or "수령" in report_name):
            return "임상시험결과"
        elif "임상" in report_name and ("철회" in report_name or "종료" in report_name):
            return "임상시험계획철회"
        # 기타 임상 관련
        else:
            return "임상관련"

    # 키워드 매칭으로 공시 유형 분류
    for disclosure_type, keywords in disclosure_keywords.items():
        for keyword in keywords:
            if keyword in report_name:
                return disclosure_type

    return "기타"


def analyze_disclosure_types_by_keywords(df, ret_columns):
    """키워드 기반 공시 유형별 분석"""
    print("\n=== 키워드 기반 공시 유형별 분석 ===")

    # 공시 유형 분류
    df["disclosure_category"] = df["report_name"].apply(classify_disclosure_type)

    # 분류된 공시 유형별 분포
    category_counts = df["disclosure_category"].value_counts()
    print(f"총 {len(category_counts)}개의 공시 카테고리가 있습니다.")
    print("\n공시 카테고리별 분포:")
    print(category_counts)

    # 수익률 데이터가 있는 공시들만 필터링
    has_return_data = df[ret_columns].notna().any(axis=1)
    df_with_returns = df[has_return_data].copy()

    print(f"\n수익률 데이터가 있는 공시 건수: {len(df_with_returns)}")
    print(f"전체 대비 비율: {len(df_with_returns)/len(df)*100:.1f}%")

    # 분류된 카테고리별 수익률 데이터 현황
    category_return_counts = df_with_returns["disclosure_category"].value_counts()
    print(f"\n수익률 데이터가 있는 공시 카테고리별 건수:")
    print(category_return_counts)

    return df_with_returns, category_counts


def calculate_return_statistics_by_category(df_with_returns, ret_columns):
    """분류된 공시 카테고리별 수익률 통계 계산"""
    print("\n=== 공시 카테고리별 수익률 통계 ===")

    # 공시 카테고리별로 그룹화하여 수익률 통계 계산
    stats_dict = {}
    for col in ret_columns:
        stats_dict[col] = ["count", "mean", "std", "min", "max"]

    category_return_stats = (
        df_with_returns.groupby("disclosure_category").agg(stats_dict).round(3)
    )

    # 컬럼명 정리
    category_return_stats.columns = [
        "_".join(col).strip() for col in category_return_stats.columns
    ]

    # 양수 비율 계산 및 추가
    positive_ratios = {}
    for col in ret_columns:
        positive_ratios[f"{col}_positive_ratio"] = (
            df_with_returns.groupby("disclosure_category")[col]
            .apply(
                lambda x: (
                    (x > 0).sum() / len(x.dropna()) * 100 if len(x.dropna()) > 0 else 0
                )
            )
            .round(1)
        )

    # 양수 비율을 기존 통계에 추가
    for col_name, ratios in positive_ratios.items():
        category_return_stats[col_name] = ratios

    # 1분 수익률 기준으로 정렬하여 상위 10개 표시
    if "ret_1m_mean" in category_return_stats.columns:
        top_categories = category_return_stats.sort_values(
            "ret_1m_mean", ascending=False
        ).head(10)
        print("\n1분 수익률 기준 상위 10개 공시 카테고리:")
        display_columns = ["ret_1m_count", "ret_1m_mean", "ret_1m_std"]
        if "ret_1m_positive_ratio" in category_return_stats.columns:
            display_columns.append("ret_1m_positive_ratio")
        print(top_categories[display_columns])

    return category_return_stats


def detailed_analysis_by_category(df_with_returns, top_n=10):
    """분류된 공시 카테고리별 상세 분석"""
    print(f"\n=== 공시 카테고리별 상세 분석 (상위 {top_n}개) ===")

    # 수익률 데이터가 가장 많은 공시 카테고리 선택
    top_categories = (
        df_with_returns["disclosure_category"].value_counts().head(top_n).index
    )

    results = {}

    for category in top_categories:
        print(f"\n--- {category} ---")
        category_data = df_with_returns[
            df_with_returns["disclosure_category"] == category
        ]

        print(f"건수: {len(category_data)}")

        # 해당 카테고리의 실제 report_name 예시
        sample_reports = category_data["report_name"].unique()[:5]
        print(f"예시 공시명: {sample_reports}")

        # 각 기간별 수익률 통계
        for period in ["ret_1m", "ret_3m", "ret_10m", "ret_60m"]:
            if period in category_data.columns:
                data = category_data[period].dropna()
                if len(data) > 0:
                    print(
                        f"{period}: 평균={data.mean():.3f}, 표준편차={data.std():.3f}"
                    )

                    # 양수/음수 수익률 비율
                    positive_count = (data > 0).sum()
                    total_count = len(data)
                    positive_ratio = positive_count / total_count * 100
                    print(
                        f"  양수 비율: {positive_count}/{total_count} ({positive_ratio:.1f}%)"
                    )

        # 결과 저장
        results[category] = {
            "count": len(category_data),
            "ret_1m_mean": (
                category_data["ret_1m"].mean()
                if "ret_1m" in category_data.columns
                else np.nan
            ),
            "ret_3m_mean": (
                category_data["ret_3m"].mean()
                if "ret_3m" in category_data.columns
                else np.nan
            ),
            "ret_10m_mean": (
                category_data["ret_10m"].mean()
                if "ret_10m" in category_data.columns
                else np.nan
            ),
            "ret_60m_mean": (
                category_data["ret_60m"].mean()
                if "ret_60m" in category_data.columns
                else np.nan
            ),
        }

    return results


def statistical_significance_test_by_category(df_with_returns, top_categories):
    """분류된 공시 카테고리별 통계적 유의성 검정"""
    print("\n=== 공시 카테고리별 통계적 유의성 검정 ===")

    for category in top_categories:
        print(f"\n{category}:")
        category_data = df_with_returns[
            df_with_returns["disclosure_category"] == category
        ]

        for period in ["ret_1m", "ret_3m", "ret_10m", "ret_60m"]:
            if period in category_data.columns:
                data = category_data[period].dropna()
                if len(data) > 1:
                    # t-검정 (평균이 0과 다른지)
                    t_stat, p_value = stats.ttest_1samp(data, 0)

                    # 유의수준 판단
                    significance = ""
                    if p_value < 0.001:
                        significance = "***"
                    elif p_value < 0.01:
                        significance = "**"
                    elif p_value < 0.05:
                        significance = "*"

                    print(f"  {period}: t={t_stat:.3f}, p={p_value:.3f} {significance}")


def create_visualizations_by_category(df_with_returns, top_categories):
    """분류된 공시 카테고리별 시각화 생성"""
    print("\n=== 공시 카테고리별 시각화 생성 ===")

    # 1. 공시 카테고리별 1개월 수익률 분포
    plt.figure(figsize=(20, 12))

    for i, category in enumerate(top_categories[:10]):  # 상위 10개만
        plt.subplot(2, 5, i + 1)
        category_data = df_with_returns[
            df_with_returns["disclosure_category"] == category
        ]

        if "ret_1m" in category_data.columns:
            data = category_data["ret_1m"].dropna()
            if len(data) > 0:
                plt.hist(data, bins=20, alpha=0.7, edgecolor="black")
                plt.title(f"{category}\n(건수: {len(data)})")
                plt.xlabel("1개월 수익률")
                plt.ylabel("빈도")
                plt.axvline(
                    data.mean(),
                    color="red",
                    linestyle="--",
                    label=f"평균: {data.mean():.3f}",
                )
                plt.legend()
                plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        "disclosure_category_returns_distribution.png", dpi=300, bbox_inches="tight"
    )
    plt.show()

    # 2. 공시 카테고리별 평균 수익률 비교
    plt.figure(figsize=(15, 8))

    categories = []
    ret_1m_means = []
    ret_3m_means = []

    for category in top_categories[:15]:  # 상위 15개만
        category_data = df_with_returns[
            df_with_returns["disclosure_category"] == category
        ]

        if "ret_1m" in category_data.columns and "ret_3m" in category_data.columns:
            ret_1m_mean = category_data["ret_1m"].mean()
            ret_3m_mean = category_data["ret_3m"].mean()

            if not pd.isna(ret_1m_mean) and not pd.isna(ret_3m_mean):
                categories.append(category)
                ret_1m_means.append(ret_1m_mean)
                ret_3m_means.append(ret_3m_mean)

    if categories:
        x = np.arange(len(categories))
        width = 0.35

        plt.bar(x - width / 2, ret_1m_means, width, label="1개월 수익률", alpha=0.8)
        plt.bar(x + width / 2, ret_3m_means, width, label="3개월 수익률", alpha=0.8)

        plt.xlabel("공시 카테고리")
        plt.ylabel("평균 수익률")
        plt.title("공시 카테고리별 평균 수익률 비교")
        plt.xticks(x, categories, rotation=45, ha="right")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        plt.savefig(
            "disclosure_category_average_returns.png", dpi=300, bbox_inches="tight"
        )
        plt.show()

    # 3. 공시 카테고리별 건수와 평균 수익률 산점도
    plt.figure(figsize=(12, 8))

    categories = []
    counts = []
    ret_1m_means = []

    for category in top_categories:
        category_data = df_with_returns[
            df_with_returns["disclosure_category"] == category
        ]

        if "ret_1m" in category_data.columns:
            ret_1m_mean = category_data["ret_1m"].mean()

            if not pd.isna(ret_1m_mean):
                categories.append(category)
                counts.append(len(category_data))
                ret_1m_means.append(ret_1m_mean)

    if categories:
        plt.scatter(counts, ret_1m_means, alpha=0.7, s=100)

        # 카테고리명 표시
        for i, category in enumerate(categories):
            plt.annotate(
                category,
                (counts[i], ret_1m_means[i]),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                ha="left",
            )

        plt.xlabel("공시 건수")
        plt.ylabel("1개월 평균 수익률")
        plt.title("공시 카테고리별 건수와 평균 수익률 관계")
        plt.grid(True, alpha=0.3)

        # 0선 추가
        plt.axhline(y=0, color="red", linestyle="--", alpha=0.7)

        plt.tight_layout()
        plt.savefig(
            "disclosure_category_count_vs_returns.png", dpi=300, bbox_inches="tight"
        )
        plt.show()


def main():
    """메인 함수"""
    print("=== 공시 유형별 주가 영향 분석 (키워드 기반 분류) ===")

    # 데이터 로드
    df = load_data()
    if df is None:
        return

    # 기본 분석
    ret_columns = basic_analysis(df)

    # 키워드 기반 공시 유형 분석
    df_with_returns, category_counts = analyze_disclosure_types_by_keywords(
        df, ret_columns
    )

    # 분류된 카테고리별 수익률 통계
    category_return_stats = calculate_return_statistics_by_category(
        df_with_returns, ret_columns
    )

    # 상세 분석
    top_categories = (
        df_with_returns["disclosure_category"].value_counts().head(10).index
    )
    detailed_results = detailed_analysis_by_category(df_with_returns, 10)

    # 통계적 유의성 검정
    statistical_significance_test_by_category(df_with_returns, top_categories)

    # 시각화
    create_visualizations_by_category(df_with_returns, top_categories)

    print("\n=== 분석 완료 ===")
    print("생성된 파일:")
    print("- disclosure_category_returns_distribution.png")
    print("- disclosure_category_average_returns.png")
    print("- disclosure_category_count_vs_returns.png")


if __name__ == "__main__":
    main()
