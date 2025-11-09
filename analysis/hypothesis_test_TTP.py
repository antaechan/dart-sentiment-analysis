import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.stats import norm


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


def regress_ln_ttp_with_volume(
    df,
    time_col="time_to_peak",
    dummy_col="telegram_dummy",
    volume_col="delta_cum_volume_10m",
    robust_se=True,
):
    """
    ln(time_to_peak) = α + β·telegram_dummy + γ·delta_cum_volume_10m + ε 회귀 실행 및 출력.

    Parameters
    ----------
    df : DataFrame
        분석 대상 데이터 (time_to_peak, telegram_dummy, delta_cum_volume_10m 포함)
    time_col : str
        시간-to-peak 컬럼명 (기본: 'time_to_peak')
    dummy_col : str
        텔레그램 더미 컬럼명 (기본: 'telegram_dummy')
    volume_col : str
        거래량 변화 컬럼명 (기본: 'delta_cum_volume_10m')
    robust_se : bool
        HC1 견고표준오차 사용 여부 (기본 True)
    """
    required = [time_col, dummy_col, volume_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for regression: {missing}")

    data = (
        df[required]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    data = data[data[time_col] > 0]
    if data.empty:
        raise ValueError("No valid observations for ln(time_to_peak) regression.")

    data = data.copy()
    data["ln_ttp"] = np.log(data[time_col])

    X = sm.add_constant(data[[dummy_col, volume_col]], has_constant="add")
    model = sm.OLS(data["ln_ttp"], X)
    results = model.fit(cov_type="HC1") if robust_se else model.fit()

    params = results.params
    bse = results.bse
    tvals = results.tvalues
    pvals = results.pvalues

    rows = []
    for term in ["const", dummy_col, volume_col]:
        coef = float(params.get(term, np.nan))
        p_val = float(pvals.get(term, np.nan))
        rows.append(
            {
                "variable": term,
                "coef_star": (
                    f"{coef:.4f}{get_sig_star(p_val)}" if np.isfinite(coef) else np.nan
                ),
                "std": float(bse.get(term, np.nan)),
                "t_stat": float(tvals.get(term, np.nan)),
                "p_value": p_val,
            }
        )

    table = pd.DataFrame(rows)
    round_map = {"std": 4, "t_stat": 3, "p_value": 12}

    print("\n--- ln(time_to_peak) 회귀 (10m) ---")
    print(
        table[["variable", "coef_star", "std", "t_stat", "p_value"]]
        .rename(columns={"coef_star": "coef"})
        .round(round_map)
        .to_string(index=False)
    )
    print(
        f"R-squared: {results.rsquared:.4f}, "
        f"Adj R-squared: {results.rsquared_adj:.4f}, "
        f"n_obs: {int(results.nobs)}"
    )

    return {
        "results": results,
        "table": table,
        "r_squared": float(results.rsquared),
        "adj_r_squared": float(results.rsquared_adj),
        "n_obs": int(results.nobs),
    }


def regress_ln_ttp(
    df,
    time_col="time_to_peak",
    dummy_col="telegram_dummy",
    robust_se=True,
):
    """
    ln(time_to_peak) = α + β·telegram_dummy + ε 회귀 실행 및 출력.

    Parameters
    ----------
    df : DataFrame
        분석 대상 데이터 (time_to_peak, telegram_dummy 포함)
    time_col : str
        시간-to-peak 컬럼명 (기본: 'time_to_peak')
    dummy_col : str
        텔레그램 더미 컬럼명 (기본: 'telegram_dummy')
    robust_se : bool
        HC1 견고표준오차 사용 여부 (기본 True)
    """
    required = [time_col, dummy_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for regression: {missing}")

    data = (
        df[required]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    data = data[data[time_col] > 0]
    if data.empty:
        raise ValueError("No valid observations for ln(time_to_peak) regression.")

    data = data.copy()
    data["ln_ttp"] = np.log(data[time_col])

    X = sm.add_constant(data[[dummy_col]], has_constant="add")
    model = sm.OLS(data["ln_ttp"], X)
    results = model.fit(cov_type="HC1") if robust_se else model.fit()

    params = results.params
    bse = results.bse
    tvals = results.tvalues
    pvals = results.pvalues

    rows = []
    for term in ["const", dummy_col]:
        coef = float(params.get(term, np.nan))
        p_val = float(pvals.get(term, np.nan))
        rows.append(
            {
                "variable": term,
                "coef_star": (
                    f"{coef:.4f}{get_sig_star(p_val)}" if np.isfinite(coef) else np.nan
                ),
                "std": float(bse.get(term, np.nan)),
                "t_stat": float(tvals.get(term, np.nan)),
                "p_value": p_val,
            }
        )

    table = pd.DataFrame(rows)
    round_map = {"std": 4, "t_stat": 3, "p_value": 12}

    print("\n--- ln(time_to_peak) 회귀 (no volume) ---")
    print(
        table[["variable", "coef_star", "std", "t_stat", "p_value"]]
        .rename(columns={"coef_star": "coef"})
        .round(round_map)
        .to_string(index=False)
    )
    print(
        f"R-squared: {results.rsquared:.4f}, "
        f"Adj R-squared: {results.rsquared_adj:.4f}, "
        f"n_obs: {int(results.nobs)}"
    )

    return {
        "results": results,
        "table": table,
        "r_squared": float(results.rsquared),
        "adj_r_squared": float(results.rsquared_adj),
        "n_obs": int(results.nobs),
    }


def regress_ttp(
    df,
    time_col="time_to_peak",
    dummy_col="telegram_dummy",
    robust_se=True,
):
    """
    TTP = α + β·telegram_dummy + ε 회귀 실행 및 출력.

    Parameters
    ----------
    df : DataFrame
        분석 대상 데이터 (time_to_peak, telegram_dummy 포함)
    time_col : str
        시간-to-peak 컬럼명 (기본: 'time_to_peak')
    dummy_col : str
        텔레그램 더미 컬럼명 (기본: 'telegram_dummy')
    robust_se : bool
        HC1 견고표준오차 사용 여부 (기본 True)
    """
    required = [time_col, dummy_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for regression: {missing}")

    data = (
        df[required]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    data = data[data[time_col] > 0]
    if data.empty:
        raise ValueError("No valid observations for time_to_peak regression.")

    data = data.copy()
    data["ttp"] = data[time_col]

    X = sm.add_constant(data[[dummy_col]], has_constant="add")
    model = sm.OLS(data["ttp"], X)
    results = model.fit(cov_type="HC1") if robust_se else model.fit()

    params = results.params
    bse = results.bse
    tvals = results.tvalues
    pvals = results.pvalues

    rows = []
    for term in ["const", dummy_col]:
        coef = float(params.get(term, np.nan))
        p_val = float(pvals.get(term, np.nan))
        rows.append(
            {
                "variable": term,
                "coef_star": (
                    f"{coef:.4f}{get_sig_star(p_val)}" if np.isfinite(coef) else np.nan
                ),
                "std": float(bse.get(term, np.nan)),
                "t_stat": float(tvals.get(term, np.nan)),
                "p_value": p_val,
            }
        )

    table = pd.DataFrame(rows)
    round_map = {"std": 4, "t_stat": 3, "p_value": 12}

    print("\n--- time_to_peak 회귀 (no volume) ---")
    print(
        table[["variable", "coef_star", "std", "t_stat", "p_value"]]
        .rename(columns={"coef_star": "coef"})
        .round(round_map)
        .to_string(index=False)
    )
    print(
        f"R-squared: {results.rsquared:.4f}, "
        f"Adj R-squared: {results.rsquared_adj:.4f}, "
        f"n_obs: {int(results.nobs)}"
    )

    return {
        "results": results,
        "table": table,
        "r_squared": float(results.rsquared),
        "adj_r_squared": float(results.rsquared_adj),
        "n_obs": int(results.nobs),
    }


def regress_ttp_with_volume(
    df,
    time_col="time_to_peak",
    dummy_col="telegram_dummy",
    volume_col="delta_cum_volume_10m",
    robust_se=True,
):
    """
    TTP = α + β·telegram_dummy + γ·delta_cum_volume_10m + ε 회귀 실행 및 출력.

    Parameters
    ----------
    df : DataFrame
        분석 대상 데이터 (time_to_peak, telegram_dummy, delta_cum_volume_10m 포함)
    time_col : str
        시간-to-peak 컬럼명 (기본: 'time_to_peak')
    dummy_col : str
        텔레그램 더미 컬럼명 (기본: 'telegram_dummy')
    volume_col : str
        거래량 변화 컬럼명 (기본: 'delta_cum_volume_10m')
    robust_se : bool
        HC1 견고표준오차 사용 여부 (기본 True)
    """
    required = [time_col, dummy_col, volume_col]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for regression: {missing}")

    data = (
        df[required]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    data = data[data[time_col] > 0]
    if data.empty:
        raise ValueError("No valid observations for ln(time_to_peak) regression.")

    data = data.copy()
    data["ttp"] = data[time_col]

    X = sm.add_constant(data[[dummy_col, volume_col]], has_constant="add")
    model = sm.OLS(data["ttp"], X)
    results = model.fit(cov_type="HC1") if robust_se else model.fit()

    params = results.params
    bse = results.bse
    tvals = results.tvalues
    pvals = results.pvalues

    rows = []
    for term in ["const", dummy_col, volume_col]:
        coef = float(params.get(term, np.nan))
        p_val = float(pvals.get(term, np.nan))
        rows.append(
            {
                "variable": term,
                "coef_star": (
                    f"{coef:.4f}{get_sig_star(p_val)}" if np.isfinite(coef) else np.nan
                ),
                "std": float(bse.get(term, np.nan)),
                "t_stat": float(tvals.get(term, np.nan)),
                "p_value": p_val,
            }
        )

    table = pd.DataFrame(rows)
    round_map = {"std": 4, "t_stat": 3, "p_value": 12}

    print("\n--- ln(time_to_peak) 회귀 (10m) ---")
    print(
        table[["variable", "coef_star", "std", "t_stat", "p_value"]]
        .rename(columns={"coef_star": "coef"})
        .round(round_map)
        .to_string(index=False)
    )
    print(
        f"R-squared: {results.rsquared:.4f}, "
        f"Adj R-squared: {results.rsquared_adj:.4f}, "
        f"n_obs: {int(results.nobs)}"
    )

    return {
        "results": results,
        "table": table,
        "r_squared": float(results.rsquared),
        "adj_r_squared": float(results.rsquared_adj),
        "n_obs": int(results.nobs),
    }
