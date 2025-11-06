import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import norm


def _mad_trimmed_abs_mean(arr, k=2.0):
    """
    MAD 기반 트리밍 후 mean(|x|) 반환.
    arr: 1D array-like
    k  : 컷오프 배수 (기본 3.0)

    절차:
      - 유한값만 사용
      - median, MAD 계산
      - |x - median| <= k * (1.4826 * MAD) 안의 값만 남김
      - 남은 표본의 mean(|x|) 리턴 (없으면 np.nan)
    """
    x = np.asarray(arr, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return np.nan, 0

    med = np.median(x)
    mad = np.median(np.abs(x - med))
    if mad == 0:
        trimmed = x  # 분산 거의 없으면 그대로
    else:
        s = 1.4826 * mad
        trimmed = x[np.abs(x - med) <= k * s]
        if trimmed.size == 0:
            trimmed = x  # 전부 잘렸으면 폴백

    return float(np.mean(np.abs(trimmed))), int(trimmed.size)


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


# --- 공통: 디자인매트릭스 빌더 (const + period_dummy + delta_cum_volume_t) ---
def _build_X_with_vol(
    df_subset, t, covar_prefix="delta_cum_volume_", add_interaction=False
):
    """
    창 t에 대응하는 거래량 공변량을 포함한 X 생성
    - covar_prefix: 'delta_cum_volume_' -> f'{prefix}{t}m' 열을 사용
    - add_interaction=True면 period_dummy × volume 상호작용항 추가
    """
    vol_col = f"{covar_prefix}{t}m"
    X = pd.DataFrame(
        {
            "const": 1.0,
            "period_dummy": df_subset["period_dummy"].astype(float),
            "vol": pd.to_numeric(df_subset[vol_col], errors="coerce"),
        }
    )
    if add_interaction:
        X["period_x_vol"] = X["period_dummy"] * X["vol"]
    # 회귀 가능한 표본만 남김
    return X.replace([np.inf, -np.inf], np.nan)


# --- ΔCAR hit (중립 포함) + 거래량 공변량 ---
def logistic_hit_delta_with_neutral_and_vol(
    df_subset,
    t,
    neutral_epsilon=None,
    alpha=0.05,
    covar_prefix="delta_cum_volume_",
    add_interaction=False,
    robust_se=True,
):
    """
    기존 logistic_hit_delta_with_neutral 에 'delta_cum_volume_{t}m' 공변량 추가
    - add_interaction=True 면 period_dummy × volume 상호작용 포함
    - robust_se=True 면 HC1 견고표준오차 사용
    """
    # ΔCAR
    delta = df_subset[f"abn_ret_{t}m"] - df_subset[f"abn_ret_minus_{t}m"]
    label_sign = df_subset["label_sign"]

    hit = np.zeros(len(df_subset), dtype=int)

    # +/- 라벨
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(delta[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )

    # 0(중립) 라벨 처리 (MAD-트리밍 기반 ε)
    mask_neutral = label_sign == 0
    calculated_epsilon = None
    if mask_neutral.any():
        delta_neu_all = delta[mask_neutral].to_numpy(dtype=float)
        if neutral_epsilon is None:
            eps, _n_kept = _mad_trimmed_abs_mean(delta_neu_all, k=2.0)
        else:
            eps = float(neutral_epsilon)
        if np.isfinite(eps):
            calculated_epsilon = eps
            finite_mask = np.isfinite(delta_neu_all)
            idx_neutral = np.flatnonzero(mask_neutral.to_numpy())
            idx_valid = idx_neutral[finite_mask]
            hit[idx_valid] = (np.abs(delta_neu_all[finite_mask]) <= eps).astype(int)
        else:
            calculated_epsilon = np.nan

    # 디자인 매트릭스 + 표본 정리
    X = _build_X_with_vol(
        df_subset, t, covar_prefix=covar_prefix, add_interaction=add_interaction
    )
    y = pd.Series(hit, index=df_subset.index).astype(float)

    # 회귀 가능 표본만 남김 (y, X 모두 유효)
    valid = y.notna() & X.notna().all(axis=1)
    Xv, yv = X.loc[valid], y.loc[valid]

    model = sm.Logit(yv, Xv).fit(disp=0)
    res = model
    if robust_se:
        res = model.get_robustcov_results(cov_type="HC1")

    # period 효과(핵심), 공변량 효과(참고)
    b_period = float(res.params["period_dummy"])
    se_period = float(res.bse["period_dummy"])
    t_period = float(res.tvalues["period_dummy"])
    p_period = float(res.pvalues["period_dummy"])
    or_period = float(np.exp(b_period))

    # 공변량 계수들(보고용)
    b_vol = float(res.params["vol"])
    se_vol = float(res.bse["vol"])
    p_vol = float(res.pvalues["vol"])

    # 상호작용 포함 시
    if add_interaction and "period_x_vol" in res.params.index:
        b_int = float(res.params["period_x_vol"])
        se_int = float(res.bse["period_x_vol"])
        p_int = float(res.pvalues["period_x_vol"])
    else:
        b_int = se_int = p_int = np.nan

    # 평균 예측 확률: period=0 vs 1
    X0 = Xv.copy()
    X0["period_dummy"] = 0
    X1 = Xv.copy()
    X1["period_dummy"] = 1
    if add_interaction and "period_x_vol" in Xv.columns:
        X0["period_x_vol"] = X0["period_dummy"] * X0["vol"]
        X1["period_x_vol"] = X1["period_dummy"] * X1["vol"]
    p0 = float(model.predict(X0).mean())
    p1 = float(model.predict(X1).mean())
    diff_pp = (p1 - p0) * 100.0

    pseudo_r2, adj_r2 = calc_pseudo_r2(model)
    beta_star = f"{b_period:.4f}{get_sig_star(p_period)}"

    out = {
        "window": t,
        "beta": b_period,
        "beta_star": beta_star,
        "std": se_period,
        "t_stat": t_period,
        "p_value": p_period,
        "odds_ratio": or_period,
        "p_before": p0,
        "p_after": p1,
        "diff_pp": diff_pp,
        "n_obs": int(model.nobs),
        "pseudo_r2": pseudo_r2,
        "adj_r2": adj_r2,
        "neutral_epsilon": np.nan,  # 기본값
        # 공변량 보고
        "beta_vol": b_vol,
        "se_vol": se_vol,
        "p_vol": p_vol,
        "beta_interaction": b_int,
        "se_interaction": se_int,
        "p_interaction": p_int,
    }
    if mask_neutral.any():
        out["neutral_epsilon"] = (
            float(calculated_epsilon) if calculated_epsilon is not None else np.nan
        )
    return out


# --- postCAR hit (중립 포함) + 거래량 공변량 ---
def logistic_hit_postCAR_with_neutral_and_vol(
    df_subset,
    h,
    neutral_epsilon=None,
    alpha=0.05,
    covar_prefix="delta_cum_volume_",
    add_interaction=False,
    robust_se=True,
):
    """
    기존 logistic_hit_postCAR_with_neutral 에 'delta_cum_volume_{h}m' 공변량 추가
    """
    abn = df_subset[f"abn_ret_{h}m"]
    label_sign = df_subset["label_sign"]
    hit = np.zeros(len(df_subset), dtype=int)

    # +/- 라벨
    mask_posneg = label_sign != 0
    hit[mask_posneg] = (np.sign(abn[mask_posneg]) == label_sign[mask_posneg]).astype(
        int
    )

    # 0(중립): ε 계산(ΔCAR=post-pre, MAD 트리밍) → postCAR 판정
    mask_neutral = label_sign == 0
    calculated_epsilon = None
    if mask_neutral.any():
        delta_all = df_subset[f"abn_ret_{h}m"] - df_subset[f"abn_ret_minus_{h}m"]
        delta_neu_all = delta_all[mask_neutral].to_numpy(dtype=float)
        if neutral_epsilon is None:
            eps, _n_kept = _mad_trimmed_abs_mean(delta_neu_all, k=2.0)
        else:
            eps = float(neutral_epsilon)
        if np.isfinite(eps):
            calculated_epsilon = eps
            abn_neu_all = abn[mask_neutral].to_numpy(dtype=float)
            finite_abn = np.isfinite(abn_neu_all)
            idx_neutral = np.flatnonzero(mask_neutral.to_numpy())
            idx_valid_abn = idx_neutral[finite_abn]
            abn_neutral_valid = abn_neu_all[finite_abn]
            hit[idx_valid_abn] = (np.abs(abn_neutral_valid) <= eps).astype(int)
        else:
            calculated_epsilon = np.nan

    # 디자인 매트릭스 + 표본 정리
    X = _build_X_with_vol(
        df_subset, h, covar_prefix=covar_prefix, add_interaction=add_interaction
    )
    y = pd.Series(hit, index=df_subset.index).astype(float)

    valid = y.notna() & X.notna().all(axis=1)
    Xv, yv = X.loc[valid], y.loc[valid]

    model = sm.Logit(yv, Xv).fit(disp=0)
    res = model
    if robust_se:
        res = model.get_robustcov_results(cov_type="HC1")

    b_period = float(res.params["period_dummy"])
    se_period = float(res.bse["period_dummy"])
    t_period = float(res.tvalues["period_dummy"])
    p_period = float(res.pvalues["period_dummy"])
    or_period = float(np.exp(b_period))

    b_vol = float(res.params["vol"])
    se_vol = float(res.bse["vol"])
    p_vol = float(res.pvalues["vol"])

    if add_interaction and "period_x_vol" in res.params.index:
        b_int = float(res.params["period_x_vol"])
        se_int = float(res.bse["period_x_vol"])
        p_int = float(res.pvalues["period_x_vol"])
    else:
        b_int = se_int = p_int = np.nan

    X0 = Xv.copy()
    X0["period_dummy"] = 0
    X1 = Xv.copy()
    X1["period_dummy"] = 1
    if add_interaction and "period_x_vol" in Xv.columns:
        X0["period_x_vol"] = X0["period_dummy"] * X0["vol"]
        X1["period_x_vol"] = X1["period_dummy"] * X1["vol"]
    p0 = float(model.predict(X0).mean())
    p1 = float(model.predict(X1).mean())
    diff_pp = (p1 - p0) * 100.0

    pseudo_r2, adj_r2 = calc_pseudo_r2(model)
    beta_star = f"{b_period:.4f}{get_sig_star(p_period)}"

    out = {
        "window": h,
        "beta": b_period,
        "beta_star": beta_star,
        "std": se_period,
        "t_stat": t_period,
        "p_value": p_period,
        "odds_ratio": or_period,
        "p_before": p0,
        "p_after": p1,
        "diff_pp": diff_pp,
        "n_obs": int(model.nobs),
        "pseudo_r2": pseudo_r2,
        "adj_r2": adj_r2,
        "neutral_epsilon": np.nan,
        "beta_vol": b_vol,
        "se_vol": se_vol,
        "p_vol": p_vol,
        "beta_interaction": b_int,
        "se_interaction": se_int,
        "p_interaction": p_int,
    }
    if mask_neutral.any():
        out["neutral_epsilon"] = (
            float(calculated_epsilon) if calculated_epsilon is not None else np.nan
        )
    return out


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
