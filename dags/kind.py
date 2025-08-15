"""
Airflow DAG (TaskFlow API): Chrome(Selenium)으로 KIND 상세검색 페이지에 접속해
'공시 발표 시간(접수/제출일시), 회사명(법인/제출인), 공시 제목(보고서명)'을 크롤링하여 CSV로 저장합니다.

요구 사항
- Airflow 2.x (TaskFlow API)
- selenium, webdriver-manager

설치 예)
  pip install "apache-airflow==2.*" selenium webdriver-manager

배치 경로 예)
  $AIRFLOW_HOME/dags/kind_disclosure_crawl_dag.py

환경 변수(옵션)
  KIND_TARGET_URL : 기본값 'https://kind.krx.co.kr/disclosure/details.do?method=searchDetailsMain#viewer'
  OUTPUT_DIR      : 기본값 '/opt/airflow/dags/data'
  HEADLESS        : 'true'/'false' (기본 true)
  MAX_PAGES       : 최대 페이지 크롤 수 (기본 5)
"""

from __future__ import annotations
import os
import csv
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

import pendulum

from airflow.decorators import dag, task

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------- 설정값 ----------------------
DEFAULT_URL = os.environ.get(
    "KIND_TARGET_URL",
    "https://kind.krx.co.kr/disclosure/details.do?method=searchDetailsMain#viewer",
)
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/airflow/dags/data")
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"
MAX_PAGES = int(os.environ.get("MAX_PAGES", "5"))

# ---------------------- Selenium 도우미 ----------------------


def _setup_driver() -> webdriver.Chrome:
    opts = ChromeOptions()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    prefs = {
        "profile.default_content_setting_values.automatic_downloads": 1,
        "download.prompt_for_download": False,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(2)
    return driver


def _close_possible_popups(driver: webdriver.Chrome):
    candidates = [
        (By.XPATH, "//button[contains(., '닫기')]"),
        (By.XPATH, "//a[contains(., '닫기')]"),
        (By.XPATH, "//div[contains(@class,'popup')]//button[contains(.,'닫기')]"),
    ]
    for by, sel in candidates:
        try:
            elems = driver.find_elements(by, sel)
            for e in elems:
                if e.is_displayed():
                    e.click()
                    time.sleep(0.5)
        except Exception:
            pass


def _click_search(driver: webdriver.Chrome):
    search_xpaths = [
        "//button[contains(., '검색')]",
        "//a[contains(., '검색')]",
        "//input[@type='button' and contains(@value,'검색')]",
    ]
    for xp in search_xpaths:
        try:
            btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, xp))
            )
            btn.click()
            return
        except Exception:
            continue


def _wait_results_table(driver: webdriver.Chrome):
    table_like = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                "(//table)[1] | //div[contains(@class,'grid') or contains(@id,'grid') or contains(@class,'result')]",
            )
        )
    )
    return table_like


def _normalize_header(t: str) -> str:
    t = (t or "").strip()
    t = t.replace("\n", " ")
    t = t.replace("  ", " ")
    return t


HEADER_ALIASES = {
    "발표시간": {"공시제출일시", "접수일시", "공시일시", "제출일시"},
    "회사명": {"법인명", "제출인명", "법인/제출인", "회사명"},
    "제목": {"보고서명", "공시제목", "제목"},
}


def _map_header_indices(driver: webdriver.Chrome) -> Dict[str, int]:
    header_cells: List[str] = []
    try:
        ths = driver.find_elements(By.XPATH, "//table//thead//th")
        if not ths:
            ths = driver.find_elements(
                By.XPATH, "(//table)[1]//tr[1]//th | (//table)[1]//tr[1]//td"
            )
        header_cells = [_normalize_header(th.text) for th in ths]
    except Exception:
        header_cells = []

    header_map: Dict[str, int] = {}
    for idx, h in enumerate(header_cells):
        for key, alias_set in HEADER_ALIASES.items():
            if h in alias_set or any(a in h for a in alias_set):
                header_map[key] = idx
    return header_map


def _extract_rows(driver: webdriver.Chrome, header_map: Dict[str, int]) -> List[dict]:
    rows = driver.find_elements(By.XPATH, "(//table)[1]//tbody//tr[td or th]")
    results: List[dict] = []

    fb_time = header_map.get("발표시간", 0)
    fb_company = header_map.get("회사명", 1)
    fb_title = header_map.get("제목", 2)

    for r in rows:
        try:
            tds = r.find_elements(By.XPATH, ".//td | .//th")
            if not tds:
                continue
            time_txt = tds[fb_time].text.strip() if fb_time < len(tds) else ""
            company_txt = tds[fb_company].text.strip() if fb_company < len(tds) else ""
            title_cell = tds[fb_title] if fb_title < len(tds) else None
            title_txt = title_cell.text.strip() if title_cell else ""

            link = ""
            try:
                a = title_cell.find_element(By.XPATH, ".//a") if title_cell else None
                if a:
                    link = a.get_attribute("href") or ""
            except Exception:
                link = ""

            if any([time_txt, company_txt, title_txt]):
                results.append(
                    {
                        "announced_at": time_txt,
                        "company": company_txt,
                        "title": title_txt,
                        "detail_url": link,
                    }
                )
        except Exception:
            continue
    return results


def _click_next_page(driver: webdriver.Chrome) -> bool:
    candidates = [
        "//a[contains(., '다음') and not(contains(@class,'disabled'))]",
        "//button[contains(., '다음') and not(@disabled)]",
        "//a[@aria-label='다음']",
        "//a[contains(@class,'next') and not(contains(@class,'disabled'))]",
        "//button[contains(@class,'next') and not(@disabled)]",
    ]
    for xp in candidates:
        try:
            next_btn = driver.find_element(By.XPATH, xp)
            if next_btn and next_btn.is_displayed():
                driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(1.2)
                return True
        except Exception:
            continue
    return False


def _crawl_kind_to_csv(
    target_url: Optional[str] = None,
    output_dir: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> str:
    url = target_url or DEFAULT_URL
    out_dir = output_dir or OUTPUT_DIR
    pages = max_pages or MAX_PAGES

    os.makedirs(out_dir, exist_ok=True)

    driver = _setup_driver()
    all_rows: List[dict] = []
    try:
        logging.info(f"Open: {url}")
        driver.get(url)
        time.sleep(1.2)
        _close_possible_popups(driver)

        if not driver.title or "상세검색" not in driver.page_source:
            try:
                elem = driver.find_element(By.XPATH, "//a[contains(., '상세검색')]")
                elem.click()
                time.sleep(1.0)
            except Exception:
                try:
                    elem2 = driver.find_element(
                        By.XPATH, "//a[contains(., '오늘의공시')]"
                    )
                    elem2.click()
                    time.sleep(1.0)
                except Exception:
                    pass

        _click_search(driver)
        _wait_results_table(driver)
        header_map = _map_header_indices(driver)

        page_count = 0
        while True:
            _wait_results_table(driver)
            rows = _extract_rows(driver, header_map)
            all_rows.extend(rows)
            page_count += 1
            if page_count >= pages:
                break
            if not _click_next_page(driver):
                break

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        outfile = os.path.join(out_dir, f"kind_disclosures_{ts}.csv")
        with open(outfile, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f, fieldnames=["announced_at", "company", "title", "detail_url"]
            )
            writer.writeheader()
            for r in all_rows:
                writer.writerow(r)

        logging.info(f"Saved {len(all_rows)} rows to {outfile}")
        return outfile

    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ---------------------- Airflow DAG (TaskFlow) ----------------------
local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id="kind_disclosure_crawl_dag",
    description="KIND 상세검색에서 공시 발표시간/회사명/제목을 수집해 CSV로 저장 (TaskFlow)",
    start_date=pendulum.datetime(2025, 8, 1, tz=local_tz),
    schedule="0 * * * *",  # 매 정시마다
    catchup=False,
    tags=["KIND", "selenium", "disclosure"],
)
def kind_disclosure_crawl_dag():
    @task(retries=1, retry_delay=pendulum.duration(minutes=2))
    def crawl_to_csv() -> str:
        outfile = _crawl_kind_to_csv()
        print(f"CSV saved to: {outfile}")
        return outfile

    crawl_to_csv()


dag = kind_disclosure_crawl_dag()
