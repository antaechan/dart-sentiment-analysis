import numpy as np
import pandas as pd
import statsmodels.api as sm


def _safe_std_1d(a):
    """NaN 무시, 길이<=1이면 0 반환"""
    a = np.asarray(a, dtype=float)
    a = a[~np.isnan(a)]
    n = a.size
    if n <= 1:
        return 0.0
    return float(np.nanstd(a, ddof=1))


def _eventwise_se_delta(df_subset, t):
    """
    이벤트별 ΔCAR의 SE (근사):
      - pre:  abn_ret_minus_1m ... abn_ret_minus_tm
      - post: abn_ret_1m ... abn_ret_tm
    SE_pre  ~= SD(pre_window)  * sqrt(t)
    SE_post ~= SD(post_window) * sqrt(t)
    SE_delta_i = sqrt(SE_pre^2 + SE_post^2)
    """
    pre_cols = [f"abn_ret_minus_{k}m" for k in range(1, t + 1)]
    post_cols = [f"abn_ret_{k}m" for k in range(1, t + 1)]

    pre_mat = df_subset[pre_cols].to_numpy(dtype=float)
    post_mat = df_subset[post_cols].to_numpy(dtype=float)

    # 이벤트별 표준편차
    sd_pre = np.apply_along_axis(_safe_std_1d, 1, pre_mat)
    sd_post = np.apply_along_axis(_safe_std_1d, 1, post_mat)

    se_pre = sd_pre * np.sqrt(t)
    se_post = sd_post * np.sqrt(t)
    se_delta = np.sqrt(se_pre**2 + se_post**2)

    # ΔCAR
    delta = post_mat.sum(axis=1) - pre_mat.sum(axis=1)
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


def logistic_hit_delta_with_neutral(df_subset, t, neutral_epsilon=0.8):
    """
    ΔCAR 기준: ΔCAR_{i,t} = CAR_{post,i,t} - CAR_{pre,i,t}
    hit:
      - label=+1 or -1: hit=1 if sign(ΔCAR) == label_sign
      - label=0(중립): hit=1 if |ΔCAR| <= neutral_epsilon
    logistic regression on period_dummy
    """
    delta = df_subset[f"abn_ret_{t}m"] - df_subset[f"abn_ret_minus_{t}m"]
    label_sign = df_subset["label_sign"]
    hit = np.zeros(len(df_subset), dtype=int)
    # Positive/Negative
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(delta[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )
    # Neutral
    mask_neutral = label_sign == 0
    hit[mask_neutral] = (np.abs(delta[mask_neutral]) <= neutral_epsilon).astype(int)

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


def logistic_hit_postCAR_with_neutral(df_subset, h, neutral_epsilon=0.4):
    """
    post CAR 기준: 0→+h 누적초과수익의 부호 부합 여부
    hit:
      - label=+1 or -1: hit=1 if sign(abn_ret_{h}m) == label_sign
      - label=0(중립): hit=1 if |abn_ret_{h}m| <= neutral_epsilon
    logistic regression on period_dummy
    """
    abn = df_subset[f"abn_ret_{h}m"]
    label_sign = df_subset["label_sign"]
    hit = np.zeros(len(df_subset), dtype=int)
    # Positive/Negative
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(abn[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )
    # Neutral
    mask_neutral = label_sign == 0
    hit[mask_neutral] = (np.abs(abn[mask_neutral]) <= neutral_epsilon).astype(int)

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
