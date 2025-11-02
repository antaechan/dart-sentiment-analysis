import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import norm


def _safe_std_1d(a):
    """NaN 무시, 길이<=1이면 0 반환"""
    a = np.asarray(a, dtype=float)
    a = a[~np.isnan(a)]
    n = a.size
    if n <= 1:
        return 0.0
    return float(np.nanstd(a, ddof=1))


def _eventwise_se_delta(df_subset, event_window):
    """
    이벤트별 ΔCAR의 SE (근사):
      - pre:  abn_ret_minus_{event_window}m 단일 컬럼 사용
      - post: abn_ret_{event_window}m 단일 컬럼 사용
      - 전체 표본의 표준편차를 기반으로 모든 이벤트에 동일한 SE 적용
    SE_pre  ~= SD_global(pre_window)
    SE_post ~= SD_global(post_window)
    SE_delta_i = sqrt(SE_pre^2 + SE_post^2) (모든 이벤트 동일)

    Parameters:
    -----------
    df_subset : DataFrame
        이벤트 데이터프레임
    event_window : int
        이벤트 창 크기 (분 단위)

    Returns:
    --------
    se_delta : ndarray
        각 이벤트별 SE (모든 이벤트에 동일한 값)
    delta : ndarray
        각 이벤트별 ΔCAR 값
    """
    pre_col = f"abn_ret_minus_{event_window}m"
    post_col = f"abn_ret_{event_window}m"

    if pre_col not in df_subset.columns or post_col not in df_subset.columns:
        raise KeyError(
            f"Required columns '{pre_col}' or '{post_col}' not found in dataframe. "
            f"Available columns: {list(df_subset.columns)}"
        )

    # 전체 표본의 표준편차 계산
    pre_vals = df_subset[pre_col].dropna()
    post_vals = df_subset[post_col].dropna()

    sd_pre_global = pre_vals.std(ddof=1) if len(pre_vals) > 1 else 0.0
    sd_post_global = post_vals.std(ddof=1) if len(post_vals) > 1 else 0.0

    # 모든 이벤트에 동일한 SE 적용
    n = len(df_subset)
    se_pre = np.full(n, sd_pre_global)
    se_post = np.full(n, sd_post_global)
    se_delta = np.sqrt(se_pre**2 + se_post**2)

    # ΔCAR
    delta = df_subset[post_col].fillna(0) - df_subset[pre_col].fillna(0)
    delta = delta.to_numpy(dtype=float)

    return se_delta, delta


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
      - label=+1 or -1: hit=1 if sign(ΔCAR) == label_sign
      - label=0(중립): hit=1 if |ΔCAR| <= eps_i (이벤트별 epsilon 자동 계산)

    Parameters:
    -----------
    neutral_epsilon : float, optional
        중립 이벤트 epsilon 값. None이면 자동 계산 (기본값: None)
    alpha : float
        유의수준 (기본값: 0.05)
    """
    delta = df_subset[f"abn_ret_{t}m"] - df_subset[f"abn_ret_minus_{t}m"]
    label_sign = df_subset["label_sign"]
    hit = np.zeros(len(df_subset), dtype=int)
    # Positive/Negative
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(delta[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )
    # Neutral: epsilon 자동 계산
    mask_neutral = label_sign == 0
    if mask_neutral.sum() > 0:
        if neutral_epsilon is None:
            # 이벤트별 SE 계산하여 epsilon 자동 계산
            z = norm.ppf(1 - alpha / 2.0)
            # 전체 데이터프레임에 대해 SE 계산 (인덱스 보존)
            se_delta_all, _ = _eventwise_se_delta(df_subset, event_window=t)
            # 중립 이벤트만 필터링
            eps_i_neutral = z * se_delta_all[mask_neutral]
            # delta 값을 가져와서 비교
            delta_neutral = delta[mask_neutral].to_numpy()
            hit[mask_neutral] = (np.abs(delta_neutral) <= eps_i_neutral).astype(int)
        else:
            # 기존 방식: 고정 epsilon 사용
            hit[mask_neutral] = (np.abs(delta[mask_neutral]) <= neutral_epsilon).astype(
                int
            )

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
    pseudo_r2, adj_r2 = calc_pseudo_r2(model)
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
        "pseudo_r2": pseudo_r2,
        "adj_r2": adj_r2,
    }


def logistic_hit_postCAR_with_neutral(df_subset, h, neutral_epsilon=None, alpha=0.05):
    """
    post CAR 기준: 0→+h 누적초과수익의 부호 부합 여부
    hit:
      - label=+1 or -1: hit=1 if sign(abn_ret_{h}m) == label_sign
      - label=0(중립): hit=1 if |abn_ret_{h}m| <= eps_i (전체 표본 기준 epsilon 자동 계산)

    Parameters:
    -----------
    neutral_epsilon : float, optional
        중립 이벤트 epsilon 값. None이면 자동 계산 (기본값: None)
    alpha : float
        유의수준 (기본값: 0.05)
    """
    abn = df_subset[f"abn_ret_{h}m"]
    label_sign = df_subset["label_sign"]
    hit = np.zeros(len(df_subset), dtype=int)
    # Positive/Negative
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(abn[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )
    # Neutral: epsilon 자동 계산
    mask_neutral = label_sign == 0
    if mask_neutral.sum() > 0:
        if neutral_epsilon is None:
            # post CAR는 단일 값이므로, pre/post 표준편차로 SE 계산
            z = norm.ppf(1 - alpha / 2.0)
            pre_col = f"abn_ret_minus_{h}m"
            post_col = f"abn_ret_{h}m"

            # 전체 표본의 표준편차 계산 (delta SE 근사)
            pre_vals = df_subset[pre_col].dropna()
            post_vals = df_subset[post_col].dropna()
            sd_pre = pre_vals.std(ddof=1) if len(pre_vals) > 1 else 0.0
            sd_post = post_vals.std(ddof=1) if len(post_vals) > 1 else 0.0
            se_delta_global = (
                np.sqrt(sd_pre**2 + sd_post**2) if (sd_pre > 0 or sd_post > 0) else 0.0
            )

            # 모든 중립 이벤트에 동일한 epsilon 적용
            eps_i = z * se_delta_global if se_delta_global > 0 else np.inf
            abn_neutral = abn[mask_neutral].to_numpy()
            hit[mask_neutral] = (np.abs(abn_neutral) <= eps_i).astype(int)
        else:
            # 기존 방식: 고정 epsilon 사용
            hit[mask_neutral] = (np.abs(abn[mask_neutral]) <= neutral_epsilon).astype(
                int
            )

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
    pseudo_r2, adj_r2 = calc_pseudo_r2(model)
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
        "pseudo_r2": pseudo_r2,
        "adj_r2": adj_r2,
    }


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

    print(f"\n--- {label} 기준 결과 ---")
    print(
        results_df[columns]
        .rename(columns=rename_dict)
        .round(round_dict)
        .to_string(index=False)
    )
    return results_df


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
