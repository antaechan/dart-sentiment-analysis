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
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import re
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    JavascriptException,
)
from database import (
    create_database_engine,
    create_kind_table_if_not_exists,
    _insert_rows_to_kind_table,
    get_stock_code_by_company_name,
)

_DOCNO_RE = re.compile(r"openDisclsViewer\('([^']+)'")
_CO_RE = re.compile(r"companysummary_open\('([^']+)'\)")

# ---------------------- 설정값 ----------------------
DEFAULT_URL = os.environ.get(
    "KIND_TARGET_URL",
    "https://kind.krx.co.kr/disclosure/details.do?method=searchDetailsMain#viewer",
)
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/airflow/dags/data")
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"
MAX_PAGES = int(os.environ.get("MAX_PAGES", "1000"))

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


def set_page_size(driver):
    wait = WebDriverWait(driver, 10)
    container = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.info.type-00"))
    )
    select_el = container.find_element(By.CSS_SELECTOR, "select#currentPageSize")
    go_btn = container.find_element(By.CSS_SELECTOR, "a.btn-sprite.btn-go[title='GO']")

    try:
        Select(select_el).select_by_value("100")
    except Exception:
        driver.execute_script(
            """
            const sel = arguments[0];
            sel.value = '100';
            sel.dispatchEvent(new Event('change', {bubbles:true}));
        """,
            select_el,
        )

    # GO 클릭
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", go_btn)
    try:
        go_btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", go_btn)


def set_date_by_typing(driver, start_date: str, end_date: str):
    """상세검색 폼의 기간 입력칸(#fromDate, #toDate)에 값 설정 후 안정화 대기."""
    if not (start_date or end_date):
        return

    sd = start_date
    ed = end_date

    # 입력칸 존재 대기
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "fromDate")))
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "toDate")))

    # 달력 위젯/마스크가 걸려 있어도 확실히 반영되도록 JS로 value 세팅 + 이벤트 발생
    js = r"""
    const sd = arguments[0]; const ed = arguments[1];
    const fd = document.getElementById('fromDate');
    const td = document.getElementById('toDate');

    function setVal(el, val){
      if (!el) return;
      el.focus();
      el.value = val || '';
      // jQuery/원시 모두 대응: input/change 이벤트 발생시켜 내부 검증/바인딩 태우기
      const ev1 = new Event('input', { bubbles: true });
      const ev2 = new Event('change', { bubbles: true });
      el.dispatchEvent(ev1); el.dispatchEvent(ev2);
    }

    setVal(fd, sd);
    setVal(td, ed);

    // KIND 쪽에서 쓰는 검증/마스크 함수가 있으면 시도 (없어도 무해)
    try { if (sd) chkDate('document.searchForm.fromDate'); } catch(e) {}
    try { if (ed) chkDate('document.searchForm.toDate'); } catch(e) {}
    """
    driver.execute_script(js, sd, ed)

    # 값 반영 확인(명시적 확인)
    def _ok(drv):
        v1 = drv.find_element(By.ID, "fromDate").get_attribute("value")
        v2 = drv.find_element(By.ID, "toDate").get_attribute("value")
        return (sd == "" or v1 == sd) and (ed == "" or v2 == ed)

    WebDriverWait(driver, 5).until(_ok)


def set_search_type(driver, type_codes=("01",), select_all=True):
    """
    공시유형을 JS로 직접 세팅.
    - type_codes: 문자열 코드 리스트. 예: ("01","02","05",...)
    - select_all: 각 탭에서 '전체선택' 체크할지 여부
    """
    logging.info(
        f"Setting search type with codes: {type_codes}, select_all: {select_all}"
    )

    # 0) '공시유형 초기화' 체크 해제 (기본 체크되어 있으면 검색 때 초기화됨)
    try:
        logging.debug("Attempting to uncheck disclosure type initialization...")
        driver.execute_script(
            """
            var b = document.getElementById('bfrDsclsType');
            if (b && b.checked) { b.click(); }
        """
        )
        logging.debug("Disclosure type initialization unchecked successfully")
    except Exception as e:
        logging.warning(f"Failed to uncheck disclosure type initialization: {e}")
        pass

    for code in type_codes:
        logging.info(f"Processing type code: {code}")

        # 1) 탭 열기 (사이트 내장 함수 호출)
        logging.debug(f"Opening disclosure type tab for code: {code}")
        driver.execute_script(
            """
            if (typeof fnDisclosureType === 'function') { fnDisclosureType(arguments[0]); }
        """,
            code,
        )

        # 2) 레이어가 열렸는지 대기
        layer_id = f"dsclsLayer{code}"
        logging.debug(f"Waiting for layer {layer_id} to be visible...")
        try:
            WebDriverWait(driver, timeout=5).until(
                lambda d: d.execute_script(
                    "var el=document.getElementById(arguments[0]);"
                    "return el && (el.style.display==='' || el.style.display==='block');",
                    layer_id,
                )
            )
            logging.debug(f"Layer {layer_id} is now visible")
        except Exception as e:
            logging.warning(f"Layer {layer_id} not found or not visible: {e}")
            # 레이어 없는 유형도 존재할 수 있으니 스킵
            continue

        if select_all:
            logging.debug(f"Selecting all items in layer {layer_id}...")
            # 3) '전체선택' 체크 + 사이트 함수 호출
            driver.execute_script(
                """
                var all = document.getElementById('dsclsLayer'+arguments[0]+'_all');
                if (all && !all.checked) { all.checked = true; }
                if (typeof fnCheckAllDscls === 'function') { fnCheckAllDscls(arguments[0]); }
            """,
                code,
            )

            # 4) 체크가 실제 반영됐는지 검증 (체크된 항목 수 > 0)
            try:
                logging.debug(
                    f"Verifying that items are checked in layer {layer_id}..."
                )
                WebDriverWait(driver, timeout=5).until(
                    lambda d: d.execute_script(
                        """
                        var layer = document.getElementById('dsclsLayer'+arguments[0]);
                        if(!layer) return false;
                        var qs = layer.querySelectorAll("input[type=checkbox][name^='disclosureTypeArr']:checked");
                        return qs && qs.length > 0;
                    """,
                        code,
                    )
                )
                logging.debug(f"Items successfully checked in layer {layer_id}")
            except Exception as e:
                logging.warning(
                    f"Could not verify checked items in layer {layer_id}: {e}"
                )
                # 일부 유형은 세부 항목이 없을 수도 있음 → 경고만
                pass

    logging.info("Search type setting completed")


def _click_search(driver: webdriver.Chrome):
    logging.info("Looking for search button...")
    search_btn = driver.find_element(By.CSS_SELECTOR, "a.search-btn[title='검색']")
    logging.info(
        f"Search button found: {search_btn.tag_name}, text: '{search_btn.text}', enabled: {search_btn.is_enabled()}"
    )

    # 클릭 전 상태 확인
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", search_btn)
    WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.search-btn[title='검색']"))
    )

    logging.info("Clicking search button...")
    try:
        search_btn.click()
        logging.info("Search button clicked successfully")
    except Exception as e:
        logging.warning(f"Regular click failed: {e}, trying JavaScript click...")
        driver.execute_script("arguments[0].click();", search_btn)
        logging.info("JavaScript click executed")

    # 클릭 후 페이지 변화 확인
    time.sleep(1)
    current_url = driver.current_url
    logging.info(f"Current URL after click: {current_url}")


def _wait_results_table(driver: webdriver.Chrome):
    time.sleep(5)
    table_like = WebDriverWait(driver, 6).until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                "(//table)[1] | //div[contains(@class,'grid') or contains(@id,'grid') or contains(@class,'result')]",
            )
        )
    )

    return table_like


def _safe_text(el):
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def _extract_rows(driver: webdriver.Chrome) -> List[dict]:
    rows_sel = "table.list.type-00.mt10 > tbody > tr"
    out = []
    rows = driver.find_elements(By.CSS_SELECTOR, rows_sel)
    n = len(rows)

    for i in range(n):  # 0..n-1
        for attempt in range(3):  # stale이면 최대 3회 재시도
            try:
                row = driver.find_element(
                    By.CSS_SELECTOR, f"{rows_sel}:nth-child({i+1})"
                )
                tds = row.find_elements(By.CSS_SELECTOR, "td")
                if len(tds) < 5:
                    raise Exception("not enough tds")

                disclosed_at = _safe_text(tds[1])

                # 회사
                try:
                    market = (
                        tds[2].find_element(By.CSS_SELECTOR, "img").get_attribute("alt")
                    )

                    if market == "유가증권":
                        market = "KOSPI"
                    elif market == "코스닥":
                        market = "KOSDAQ"
                    elif market == "코넥스":
                        market = "KONEX"
                    else:
                        market = "UNKNOWN"

                    a_company = tds[2].find_element(By.CSS_SELECTOR, "a#companysum")
                    company_name_txt = a_company.get_attribute("title") or _safe_text(
                        a_company
                    )
                    onclick_company = a_company.get_attribute("onclick") or ""
                except Exception:
                    a_company = None
                    company_name_txt = _safe_text(tds[2])
                    onclick_company = ""
                mco = re.search(r"companysummary_open\('([^']+)'\)", onclick_company)

                # 제목
                try:
                    a_title = tds[3].find_element(
                        By.CSS_SELECTOR, "a[onclick*='openDisclsViewer']"
                    )
                    title_txt = a_title.get_attribute("title") or _safe_text(a_title)
                    onclick_title = a_title.get_attribute("onclick") or ""
                except Exception:
                    a_title = None
                    title_txt = _safe_text(tds[3])
                    onclick_title = ""
                mdoc = re.search(r"openDisclsViewer\('([^']+)'", onclick_title)
                disclosure_id = mdoc.group(1) if mdoc else ""

                detail_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&acptno={disclosure_id}&docno=&viewerhost=&viewerport="

                out.append(
                    {
                        "disclosed_at": disclosed_at,
                        "company_name": company_name_txt,
                        "stock_code": get_stock_code_by_company_name(company_name_txt)[
                            0
                        ]
                        or "",
                        "short_code": get_stock_code_by_company_name(company_name_txt)[
                            1
                        ]
                        or "",
                        "market": market,
                        "title": title_txt,
                        "disclosure_id": disclosure_id,
                        "detail_url": detail_url,
                    }
                )
                break  # 성공했으면 다음 행으로
            except StaleElementReferenceException:
                if attempt == 2:
                    logging.warning(f"Row {i+1}: stale after retries, skipping")
                else:
                    time.sleep(0.1)  # 아주 짧은 백오프 후 재시도
                continue
            except Exception as e:
                logging.warning(f"Row {i+1} parse error: {e}")
                break
    return out


def _click_next_page(driver: webdriver.Chrome) -> bool:
    """페이징 영역의 '다음 페이지' 링크를 클릭."""
    try:
        next_btn = driver.find_element(By.CSS_SELECTOR, "div.paging a.next")
        if not next_btn.is_displayed():
            return False

        before_html = driver.page_source  # 페이지 전환 감지용
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", next_btn
        )
        driver.execute_script("arguments[0].click();", next_btn)

        # 페이지 내용이 바뀔 때까지 대기
        WebDriverWait(driver, 1).until(lambda d: d.page_source != before_html)
        return True
    except Exception:
        return False


def _crawl_kind_to_csv(
    target_url: Optional[str] = None,
    output_dir: Optional[str] = None,
    max_pages: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:

    url = target_url or DEFAULT_URL
    out_dir = output_dir or OUTPUT_DIR
    pages = max_pages or MAX_PAGES

    logging.info(f"=== Starting KIND crawling process ===")
    logging.info(f"Target URL: {url}")
    logging.info(f"Output directory: {out_dir}")
    logging.info(f"Max pages: {pages}")
    logging.info(f"Date range: {start_date} ~ {end_date}")

    # 데이터베이스 연결 및 테이블 확인
    engine = create_database_engine()
    create_kind_table_if_not_exists(engine)
    logging.info("Database connection established and kind table verified")

    os.makedirs(out_dir, exist_ok=True)
    logging.info(f"Output directory created/verified: {out_dir}")

    driver = _setup_driver()
    logging.info("WebDriver initialized successfully")

    all_rows: List[dict] = []
    try:
        logging.info(f"Opening URL: {url}")
        driver.get(url)
        time.sleep(1.2)
        logging.info("Page loaded, waiting for content to settle")

        set_page_size(driver)
        time.sleep(1.2)
        logging.info("Page size set")

        logging.info("Setting date range...")
        set_date_by_typing(driver, start_date, end_date)
        logging.info(f"Date range set: {start_date} ~ {end_date}")

        logging.info("Setting search type...")
        set_search_type(driver, type_codes=("01", "05", "06", "08"), select_all=True)
        logging.info("Search type configured")

        logging.info("Clicking search button...")
        _click_search(driver)
        logging.info("Search button clicked")

        logging.info("Waiting for results table...")
        _wait_results_table(driver)
        logging.info("Results table found")

        page_count = 0
        while True:
            logging.info(f"--- Processing page {page_count + 1} ---")
            _wait_results_table(driver)
            logging.info("Results table loaded for current page")

            rows = _extract_rows(driver)

            # kind 테이블에 rows 업데이트
            if rows:
                try:
                    _insert_rows_to_kind_table(engine, rows)
                    logging.info(
                        f"Successfully inserted {len(rows)} rows to kind table"
                    )
                except Exception as e:
                    logging.error(f"Failed to insert rows to kind table: {str(e)}")

            logging.info(f"Extracted {len(rows)} rows from page {page_count + 1}")

            all_rows.extend(rows)
            page_count += 1

            logging.info(
                f"Page {page_count}: Extracted {len(rows)} rows (Total: {len(all_rows)} rows)"
            )

            if page_count >= pages:
                logging.info(f"Reached max pages limit ({pages}), stopping pagination")
                break

            logging.info("Attempting to go to next page...")
            if not _click_next_page(driver):
                logging.info("No more pages available, stopping pagination")
                break
            else:
                logging.info("Successfully moved to next page")

        logging.info(f"=== Crawling completed ===")
        logging.info(f"Total pages processed: {page_count}")
        logging.info(f"Total rows collected: {len(all_rows)}")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        outfile = os.path.join(out_dir, f"kind_disclosures_{ts}.csv")
        logging.info(f"Preparing to save CSV to: {outfile}")

        with open(outfile, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "disclosed_at",
                    "company_name",
                    "stock_code",
                    "short_code",
                    "market",
                    "title",
                    "disclosure_id",
                    "detail_url",
                ],
            )
            writer.writeheader()
            for r in all_rows:
                writer.writerow(r)

        logging.info(f"CSV file saved successfully: {outfile}")
        logging.info(f"File size: {os.path.getsize(outfile)} bytes")
        return outfile

    except Exception as e:
        logging.error(f"Error during crawling process: {str(e)}")
        logging.error(f"Current page count: {page_count}")
        logging.error(f"Rows collected so far: {len(all_rows)}")
        raise e
    finally:
        try:
            logging.info("Closing WebDriver...")
            driver.quit()
            logging.info("WebDriver closed successfully")
        except Exception as e:
            logging.warning(f"Error closing WebDriver: {str(e)}")
            pass


# ---------------------- Airflow DAG (TaskFlow) ----------------------
local_tz = pendulum.timezone("Asia/Seoul")


@dag(
    dag_id="kind_disclosure_crawl_dag",
    description="KIND 상세검색에서 공시 발표시간/회사명/제목을 수집해 CSV로 저장 (TaskFlow)",
    start_date=pendulum.datetime(2025, 8, 1, tz=local_tz),
    schedule="@once",
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

            outfile = _crawl_kind_to_csv(
                start_date="2021-01-01",
                end_date="2021-06-30",
            )
            logging.info(f"CSV saved successfully to: {outfile}")
            print(f"CSV saved to: {outfile}")
            return outfile
        except Exception as e:
            logging.error(f"Failed to crawl KIND disclosures: {str(e)}")
            raise e

    crawl_to_csv()


dag = kind_disclosure_crawl_dag()
