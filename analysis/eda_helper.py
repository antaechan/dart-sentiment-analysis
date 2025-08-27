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
from config import disclosure_keywords

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

    if (
        "기술료" in report_name
        or "마일스톤 입금" in report_name
        or "계약금" in report_name
    ):
        return "기술료수령"

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

    # count 순서로 내림차순 정렬 (첫 번째 수익률 컬럼의 count 기준)
    first_ret_col = ret_columns[0]
    count_col = f"{first_ret_col}_count"
    category_return_stats = category_return_stats.sort_values(
        count_col, ascending=False
    )

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
