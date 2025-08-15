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

ARM64 (Apple Silicon/M1) 호환성 주의사항:
- Docker 환경에서 실행 시 ARM64 호환 이미지 사용 필요
- 예: --platform linux/amd64 또는 ARM64 네이티브 이미지
- webdriver-manager가 자동으로 적절한 드라이버 다운로드
- 컨테이너에 Chrome/Chromium 설치 필요
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
from selenium.webdriver.chrome.service import Service
import re
from selenium.webdriver.common.by import By

_DOCNO_RE = re.compile(r"openDisclsViewer\('([^']+)'")
_CO_RE = re.compile(r"companysummary_open\('([^']+)'\)")

# ---------------------- 설정값 ----------------------
DEFAULT_URL = os.environ.get(
    "KIND_TARGET_URL",
    "https://kind.krx.co.kr/disclosure/details.do?method=searchDetailsMain#viewer",
)
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/airflow/dags/data")
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"
MAX_PAGES = int(os.environ.get("MAX_PAGES", "5"))

# Timeout configurations
PAGE_LOAD_TIMEOUT = int(os.environ.get("PAGE_LOAD_TIMEOUT", "60"))
IMPLICIT_WAIT = int(os.environ.get("IMPLICIT_WAIT", "2"))
WEBDRIVER_TIMEOUT = int(os.environ.get("WEBDRIVER_TIMEOUT", "30"))

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

    # 시스템에 설치된 ARM64 드라이버로 직접 실행
    service = Service(os.getenv("CHROMEDRIVER", "/usr/bin/chromedriver"))
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)
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
    """
    KIND 상세검색 결과 테이블(번호/시간/회사/제목/부서/버튼 6열)에 특화.
    헤더가 없거나 변해도 '열 인덱스'로 안전하게 파싱.
    """
    rows = driver.find_elements(By.XPATH, "//tbody/tr[td]")  # 결과 테이블의 본문 행들
    results: List[dict] = []

    for r in rows:
        try:
            tds = r.find_elements(By.TAG_NAME, "td")
            if len(tds) < 4:
                continue

            # 1) 발표 시간 (td[1])
            announced_at = (tds[1].text or "").strip()

            # 2) 회사명 & 회사코드 (td[2])
            company_txt = ""
            company_code = ""
            try:
                a_company = tds[2].find_element(By.XPATH, ".//a[@id='companysum']")
                company_txt = (
                    a_company.get_attribute("title") or a_company.text or ""
                ).strip()
                onclick_company = a_company.get_attribute("onclick") or ""
                mco = _CO_RE.search(onclick_company)
                if mco:
                    company_code = mco.group(1)  # 예: '10167'
            except Exception:
                # a태그가 없으면 그냥 텍스트 사용 (아이콘 제거 위해 strip)
                company_txt = (tds[2].text or "").strip()

            # 3) 공시 제목 & 문서번호 (td[3])
            title_txt = ""
            disclosure_id = ""
            detail_url = ""
            try:
                a_title = tds[3].find_element(
                    By.XPATH, ".//a[contains(@onclick,'openDisclsViewer')]"
                )
                # [정정] 같은 태그가 섞여 있을 수 있어 title 속성 우선
                title_txt = (
                    a_title.get_attribute("title") or a_title.text or ""
                ).strip()
                onclick_title = a_title.get_attribute("onclick") or ""
                mdoc = _DOCNO_RE.search(onclick_title)
                if mdoc:
                    disclosure_id = mdoc.group(1)
                    detail_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={disclosure_id}&docno=&viewerhost=&viewerport="
            except Exception:
                title_txt = (tds[3].text or "").strip()

            # 값이 하나라도 있으면 적재
            if any([announced_at, company_txt, title_txt]):
                results.append(
                    {
                        "announced_at": announced_at,
                        "company": company_txt,
                        "company_code": company_code,  # 새 필드(옵션)
                        "title": title_txt,
                        "disclosure_id": disclosure_id,  # 새 필드(옵션)
                        "detail_url": detail_url,  # 필요시 조합해 사용
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
                f,
                fieldnames=[
                    "announced_at",
                    "company",
                    "company_code",
                    "title",
                    "disclosure_id",
                    "detail_url",
                ],
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
    @task()
    def crawl_to_csv() -> str:
        try:
            logging.info("Starting KIND disclosure crawling...")
            logging.info(f"Target URL: {DEFAULT_URL}")
            logging.info(f"Output directory: {OUTPUT_DIR}")
            logging.info(f"Headless mode: {HEADLESS}")
            logging.info(f"Max pages: {MAX_PAGES}")

            outfile = _crawl_kind_to_csv()
            logging.info(f"CSV saved successfully to: {outfile}")
            print(f"CSV saved to: {outfile}")
            return outfile
        except Exception as e:
            logging.error(f"Failed to crawl KIND disclosures: {str(e)}")
            raise e

    crawl_to_csv()


dag = kind_disclosure_crawl_dag()
