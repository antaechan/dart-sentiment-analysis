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

    # 주요 공시 유형별 키워드 정의
    disclosure_keywords = {
        "실적공시": [
            "실적",
            "실적발표",
            "실적공시",
            "분기실적",
            "연간실적",
            "1분기",
            "2분기",
            "3분기",
            "4분기",
            "반기",
            "연간",
            "매출",
            "영업이익",
            "당기순이익",
            "eps",
            "roe",
            "roa",
        ],
        "배당공시": [
            "배당",
            "배당금",
            "배당률",
            "배당정책",
            "배당결정",
            "현금배당",
            "주식배당",
            "배당지급",
            "배당성향",
        ],
        "자사주매입": [
            "자사주",
            "자사주매입",
            "자사주취득",
            "자사주소각",
            "자사주처분",
            "자사주보유",
            "자사주관리",
        ],
        "유상증자": [
            "유상증자",
            "신주발행",
            "증자",
            "신주",
            "자본금증가",
            "주식발행",
            "자본확충",
            "자금조달",
        ],
        "무상증자": [
            "무상증자",
            "무상증자",
            "무상주",
            "자본잉여금",
            "자본전입",
            "무상증자결정",
        ],
        "주주총회": [
            "주주총회",
            "정기주주총회",
            "임시주주총회",
            "주주총회결의",
            "주주총회소집",
            "주주총회공고",
        ],
        "이사회": [
            "이사회",
            "이사회결의",
            "이사선임",
            "이사해임",
            "이사사임",
            "이사회소집",
        ],
        "감사": ["감사", "감사보고서", "감사인", "감사위원회", "감사보고", "감사의견"],
        "계열사": [
            "계열사",
            "관계회사",
            "연결회사",
            "지배회사",
            "피지배회사",
            "계열회사",
            "관계기업",
        ],
        "인수합병": [
            "인수",
            "합병",
            "m&a",
            "기업인수",
            "기업합병",
            "법인합병",
            "기업결합",
            "사업양수도",
        ],
        "분할": ["분할", "회사분할", "사업분할", "분할설립", "분할합병", "분할결정"],
        "해산": ["해산", "회사해산", "청산", "회사청산", "해산결정", "청산보고"],
        "상장": [
            "상장",
            "코스닥상장",
            "코스피상장",
            "상장신청",
            "상장심사",
            "상장예정",
        ],
        "상장폐지": [
            "상장폐지",
            "상장적용",
            "상장취소",
            "상장정지",
            "상장폐지예정",
            "상장적용예정",
        ],
        "정관변경": [
            "정관",
            "정관변경",
            "회사정관",
            "정관수정",
            "정관개정",
            "정관변경결의",
        ],
        "사업보고서": [
            "사업보고서",
            "사업보고",
            "사업현황",
            "사업계획",
            "사업실적",
            "사업내용",
        ],
        "감자": ["감자", "자본감소", "주식감소", "감자결정", "감자공고", "감자실시"],
        "기타재무": [
            "재무상태",
            "재무제표",
            "재무정보",
            "재무현황",
            "재무상태표",
            "손익계산서",
        ],
        "기타경영": [
            "경영진",
            "경영진변경",
            "경영진사임",
            "경영진선임",
            "경영진해임",
            "경영진변경",
        ],
    }

    # 키워드 매칭으로 공시 유형 분류
    for disclosure_type, keywords in disclosure_keywords.items():
        for keyword in keywords:
            if keyword in report_name:
                return disclosure_type

    # 특별한 패턴이 있는 경우 추가 분류
    if re.search(r"\d{4}년", report_name):
        return "연도별공시"
    elif re.search(r"\d{1,2}월", report_name):
        return "월별공시"
    elif "공고" in report_name:
        return "공고"
    elif "결의" in report_name:
        return "결의사항"
    elif "보고" in report_name:
        return "보고서"

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

    # 1개월 수익률 기준으로 정렬하여 상위 10개 표시
    if "ret_1m_mean" in category_return_stats.columns:
        top_categories = category_return_stats.sort_values(
            "ret_1m_mean", ascending=False
        ).head(10)
        print("\n1개월 수익률 기준 상위 10개 공시 카테고리:")
        print(top_categories[["ret_1m_count", "ret_1m_mean", "ret_1m_std"]])

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
