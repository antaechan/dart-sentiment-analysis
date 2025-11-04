import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import norm


def print_sample_summary_with_neutral(df, label_col="label_sign"):
    """
    중립 이벤트 포함 샘플 요약 통계 출력
    """
    n_total = len(df)
    n_neutral = (df[label_col] == 0).sum()
    n_pos = (df[label_col] == 1).sum()
    n_neg = (df[label_col] == -1).sum()

    print("=== Sample summary (including neutral) ===")
    print(f"Total events:        {n_total:,}")
    print(f"Neutrals:            {n_neutral:,}")
    print(f"  - Positive (1):    {n_pos:,}")
    print(f"  - Negative (-1):   {n_neg:,}")
    print()
    return df


def get_sig_star(p):
    """p-value에 따라 유의성 표시 반환"""
    if p < 0.001:
        return "***"
    elif p < 0.01:
        return "**"
    elif p < 0.05:
        return "*"
    else:
        return ""


def calc_pseudo_r2(model):
    """
    Computes McFadden's pseudo R-squared and adjusted R-squared for a fitted Logit model.
    """
    llf = model.llf
    llnull = model.llnull
    n = model.nobs
    k = len(model.params)
    pseudo_r2 = 1 - (llf / llnull)
    adj_r2 = 1 - ((llf - k) / llnull)
    return pseudo_r2, adj_r2


def logistic_hit_delta_with_neutral(df_subset, t, neutral_epsilon=None, alpha=0.05):
    """
    ΔCAR 기준: ΔCAR_{i,t} = CAR_{post,i,t} - CAR_{pre,i,t}
    hit:
      - label=+1/-1: sign(ΔCAR) == label_sign
      - label=0(중립): |ΔCAR| <= ε,  ε = mean(|ΔCAR|) on neutral (finite only)
    """
    # ΔCAR
    delta = df_subset[f"abn_ret_{t}m"] - df_subset[f"abn_ret_minus_{t}m"]
    label_sign = df_subset["label_sign"]

    # 초기화
    hit = np.zeros(len(df_subset), dtype=int)

    # +/- 라벨: 부호 일치
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(delta[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )

    # 0(중립) 라벨: ε 계산 및 판정
    mask_neutral = label_sign == 0
    calculated_epsilon = None
    if mask_neutral.any():
        if neutral_epsilon is None:
            # 유효한 ΔCAR만 사용해 ε 계산
            delta_neu_all = delta[mask_neutral].to_numpy(dtype=float)
            finite_mask = np.isfinite(delta_neu_all)
            delta_neu = delta_neu_all[finite_mask]

            if delta_neu.size > 0:
                eps = float(np.mean(np.abs(delta_neu)))
                calculated_epsilon = eps

                # 유효한 중립 관측에만 hit 갱신
                idx_neutral = np.flatnonzero(mask_neutral.to_numpy())
                idx_valid = idx_neutral[finite_mask]
                hit[idx_valid] = (np.abs(delta_neu) <= eps).astype(int)
            else:
                calculated_epsilon = np.nan
        else:
            # 제공된 ε 사용 (중립 ΔCAR 유효값에만 판정)
            eps = float(neutral_epsilon)
            calculated_epsilon = eps
            delta_neu_all = delta[mask_neutral].to_numpy(dtype=float)
            finite_mask = np.isfinite(delta_neu_all)
            idx_neutral = np.flatnonzero(mask_neutral.to_numpy())
            idx_valid = idx_neutral[finite_mask]
            hit[idx_valid] = (np.abs(delta_neu_all[finite_mask]) <= eps).astype(int)

    # 로지스틱 회귀
    X = sm.add_constant(df_subset["period_dummy"])
    model = sm.Logit(hit, X).fit(disp=0)

    beta = float(model.params["period_dummy"])
    pval = float(model.pvalues["period_dummy"])
    std = float(model.bse["period_dummy"])
    t_stat = float(model.tvalues["period_dummy"])
    odds_ratio = float(np.exp(beta))

    # 전/후 평균 예측확률
    X0 = X.copy()
    X0["period_dummy"] = 0
    X1 = X.copy()
    X1["period_dummy"] = 1
    p0 = float(model.predict(X0).mean())
    p1 = float(model.predict(X1).mean())
    diff_pp = (p1 - p0) * 100.0

    beta_star = f"{beta:.4f}{get_sig_star(pval)}"
    pseudo_r2, adj_r2 = calc_pseudo_r2(model)

    result = {
        "window": t,
        "beta": beta,
        "beta_star": beta_star,
        "std": std,
        "t_stat": t_stat,
        "p_value": pval,
        "odds_ratio": odds_ratio,
        "p_before": p0,
        "p_after": p1,
        "diff_pp": diff_pp,
        "n_obs": int(model.nobs),
        "pseudo_r2": pseudo_r2,
        "adj_r2": adj_r2,
        # 항상 포함
        "neutral_epsilon": (
            float(calculated_epsilon) if calculated_epsilon is not None else np.nan
        ),
    }
    return result


def logistic_hit_postCAR_with_neutral(df_subset, h, neutral_epsilon=None, alpha=0.05):
    """
    post CAR 기준: 0→+h 누적초과수익의 부호 부합 여부
    hit:
      - label=+1/-1: sign(abn_ret_{h}m) == label_sign
      - label=0(중립): |abn_ret_{h}m| <= ε,  ε = mean(|ΔCAR|) on neutral (finite only)
        (ε 계산은 중립 ΔCAR의 유효값 기반, 판정은 postCAR의 유효값에 적용)
    """
    abn = df_subset[f"abn_ret_{h}m"]
    label_sign = df_subset["label_sign"]

    # 초기화
    hit = np.zeros(len(df_subset), dtype=int)

    # +/- 라벨: 부호 일치
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(abn[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )

    # 0(중립) 라벨
    mask_neutral = label_sign == 0
    calculated_epsilon = None
    if mask_neutral.any():
        # ε 계산은 ΔCAR 유효값으로
        delta_all = df_subset[f"abn_ret_{h}m"] - df_subset[f"abn_ret_minus_{h}m"]
        delta_neu_all = delta_all[mask_neutral].to_numpy(dtype=float)
        finite_delta = np.isfinite(delta_neu_all)
        delta_neu = delta_neu_all[finite_delta]

        if neutral_epsilon is None:
            if delta_neu.size > 0:
                eps = float(np.mean(np.abs(delta_neu)))
                calculated_epsilon = eps
            else:
                calculated_epsilon = np.nan
                eps = None
        else:
            eps = float(neutral_epsilon)
            calculated_epsilon = eps

        # 판정은 postCAR의 유효값에 대해서만 수행
        if eps is not None and np.isfinite(eps):
            abn_neu_all = abn[mask_neutral].to_numpy(dtype=float)
            finite_abn = np.isfinite(abn_neu_all)

            idx_neutral = np.flatnonzero(mask_neutral.to_numpy())
            idx_valid_abn = idx_neutral[finite_abn]
            abn_neutral_valid = abn_neu_all[finite_abn]

            hit[idx_valid_abn] = (np.abs(abn_neutral_valid) <= eps).astype(int)

    # 로지스틱 회귀
    X = sm.add_constant(df_subset["period_dummy"])
    model = sm.Logit(hit, X).fit(disp=0)

    beta = float(model.params["period_dummy"])
    pval = float(model.pvalues["period_dummy"])
    std = float(model.bse["period_dummy"])
    t_stat = float(model.tvalues["period_dummy"])
    odds_ratio = float(np.exp(beta))

    # 전/후 평균 예측확률
    X0 = X.copy()
    X0["period_dummy"] = 0
    X1 = X.copy()
    X1["period_dummy"] = 1
    p0 = float(model.predict(X0).mean())
    p1 = float(model.predict(X1).mean())
    diff_pp = (p1 - p0) * 100.0

    beta_star = f"{beta:.4f}{get_sig_star(pval)}"
    pseudo_r2, adj_r2 = calc_pseudo_r2(model)

    result = {
        "window": h,
        "beta": beta,
        "beta_star": beta_star,
        "std": std,
        "t_stat": t_stat,
        "p_value": pval,
        "odds_ratio": odds_ratio,
        "p_before": p0,
        "p_after": p1,
        "diff_pp": diff_pp,
        "n_obs": int(model.nobs),
        "pseudo_r2": pseudo_r2,
        "adj_r2": adj_r2,
        # 항상 포함
        "neutral_epsilon": (
            float(calculated_epsilon) if calculated_epsilon is not None else np.nan
        ),
    }
    return result


def run_logistic_table(
    logistic_fn, df_nn, windows, label="ΔCAR", include_r2=True, **kwargs
):
    """
    창(t)별 로지스틱 회귀 수행 및 요약 테이블 리턴

    Parameters:
    -----------
    logistic_fn : callable
        로지스틱 회귀 함수
    df_nn : DataFrame
        데이터프레임
    windows : list
        시간 창 리스트
    label : str
        출력 레이블
    include_r2 : bool
        R2 및 adj_R2 포함 여부 (기본값: True)
    **kwargs
        logistic_fn에 전달할 추가 인자
    """
    results = []
    for w in windows:
        results.append(logistic_fn(df_nn, w, **kwargs))
    results_df = pd.DataFrame(results)

    # 기본 컬럼 리스트
    base_columns = [
        "window",
        "beta_star",
        "std",
        "t_stat",
        "p_value",
        "odds_ratio",
        "p_before",
        "p_after",
        "diff_pp",
        "n_obs",
    ]

    # R2 포함 여부에 따라 컬럼 선택
    if include_r2 and "pseudo_r2" in results_df.columns:
        columns = base_columns + ["pseudo_r2", "adj_r2"]
        rename_dict = {"window": "t", "pseudo_r2": "R2", "adj_r2": "adj_R2"}
        round_dict = {
            "std": 4,
            "t_stat": 3,
            "p_value": 12,
            "odds_ratio": 3,
            "p_before": 3,
            "p_after": 3,
            "diff_pp": 2,
            "R2": 4,
            "adj_R2": 4,
        }
    else:
        columns = base_columns
        rename_dict = {"window": "t"}
        round_dict = {
            "std": 4,
            "t_stat": 3,
            "p_value": 12,
            "odds_ratio": 3,
            "p_before": 3,
            "p_after": 3,
            "diff_pp": 2,
        }

    # neutral_epsilon 컬럼이 있으면 추가
    if "neutral_epsilon" in results_df.columns:
        columns.append("neutral_epsilon")
        round_dict["neutral_epsilon"] = 6

    print(f"\n--- {label} 기준 결과 ---")
    print(
        results_df[columns]
        .rename(columns=rename_dict)
        .round(round_dict)
        .to_string(index=False)
    )
    return results_df[columns]


def print_sample_summary(df, label_col="label_sign"):
    """
    중립 이벤트 제거 후 샘플 요약 통계 출력
    """
    n_total = len(df)
    n_neutral = (df[label_col] == 0).sum()
    df_nn = df[df[label_col] != 0]
    n_used = len(df_nn)
    pos_cnt = (df_nn[label_col] == 1).sum()
    neg_cnt = (df_nn[label_col] == -1).sum()

    print("=== Sample summary (neutral removed) ===")
    print(f"Total events:        {n_total:,}")
    print(f"Removed neutrals:    {n_neutral:,}")
    print(f"Used (non-neutral):  {n_used:,}")
    print(f"  - Positive (1):    {pos_cnt:,}")
    print(f"  - Negative (-1):   {neg_cnt:,}")
    print()
    return df_nn


def logistic_hit_delta(df_subset, t):
    """
    ΔCAR 기준: ΔCAR_{i,t} = CAR_{post,i,t} - CAR_{pre,i,t}
    hit_{i,t} = 1 if sign(ΔCAR_{i,t}) == label_sign_i else 0
    logistic regression on period_dummy

    Note: 중립 이벤트는 이미 제거된 상태여야 함
    """
    delta = df_subset[f"abn_ret_{t}m"] - df_subset[f"abn_ret_minus_{t}m"]
    realized_sign = np.sign(delta)
    hit = (realized_sign == df_subset["label_sign"]).astype(int)
    X = sm.add_constant(df_subset["period_dummy"])
    model = sm.Logit(hit, X).fit(disp=0)
    beta = float(model.params["period_dummy"])
    pval = float(model.pvalues["period_dummy"])
    std = float(model.bse["period_dummy"])
    t_stat = float(model.tvalues["period_dummy"])
    odds_ratio = float(np.exp(beta))
    X0 = X.copy()
    X0["period_dummy"] = 0
    X1 = X.copy()
    X1["period_dummy"] = 1
    p0 = float(model.predict(X0).mean())
    p1 = float(model.predict(X1).mean())
    diff_pp = (p1 - p0) * 100.0
    beta_star = f"{beta:.4f}{get_sig_star(pval)}"
    return {
        "window": t,
        "beta": beta,
        "beta_star": beta_star,
        "std": std,
        "t_stat": t_stat,
        "p_value": pval,
        "odds_ratio": odds_ratio,
        "p_before": p0,
        "p_after": p1,
        "diff_pp": diff_pp,
        "n_obs": int(model.nobs),
    }


def logistic_hit_postCAR(df_subset, h):
    """
    post CAR 기준: 0→+h 누적초과수익의 부호 부합 여부
    hit = 1 if sign(abn_ret_{h}m) == label_sign_i else 0
    logistic regression on period_dummy

    Note: 중립 이벤트는 이미 제거된 상태여야 함
    """
    realized_sign = np.sign(df_subset[f"abn_ret_{h}m"])
    hit = (realized_sign == df_subset["label_sign"]).astype(int)
    X = sm.add_constant(df_subset["period_dummy"])
    model = sm.Logit(hit, X).fit(disp=0)
    beta = float(model.params["period_dummy"])
    pval = float(model.pvalues["period_dummy"])
    std = float(model.bse["period_dummy"])
    t_stat = float(model.tvalues["period_dummy"])
    odds_ratio = float(np.exp(beta))
    X0 = X.copy()
    X0["period_dummy"] = 0
    X1 = X.copy()
    X1["period_dummy"] = 1
    p0 = float(model.predict(X0).mean())
    p1 = float(model.predict(X1).mean())
    diff_pp = (p1 - p0) * 100.0
    beta_star = f"{beta:.4f}{get_sig_star(pval)}"
    return {
        "window": h,
        "beta": beta,
        "beta_star": beta_star,
        "std": std,
        "t_stat": t_stat,
        "p_value": pval,
        "odds_ratio": odds_ratio,
        "p_before": p0,
        "p_after": p1,
        "diff_pp": diff_pp,
        "n_obs": int(model.nobs),
    }
