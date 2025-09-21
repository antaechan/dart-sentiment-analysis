import os
from FinanceDataReader.data import StockListing
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import pandas as pd
import logging
from datetime import datetime
from typing import List, Optional
import polars as pl
import FinanceDataReader as fdr
from sqlalchemy import text
import io
import zipfile
import requests
import xml.etree.ElementTree as ET

load_dotenv()

KRX_LISTING = fdr.StockListing("KRX")[["Code", "ISU_CD", "Name", "Market"]]


def get_database_url(host: str = "postgres_events") -> str:
    """데이터베이스 연결 URL을 생성합니다."""
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

    return f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{host}:{POSTGRES_PORT}/{POSTGRES_DB}"


def create_database_engine(host: str = "postgres_events"):
    """데이터베이스 엔진을 생성합니다."""
    db_url = get_database_url(host)
    return sa.create_engine(db_url, pool_pre_ping=True, future=True)


def create_kind_table_if_not_exists(engine: sa.engine.Engine) -> bool:
    """
    CSV 파일과 동일한 필드를 가진 'kind' 테이블이 데이터베이스에 없으면 생성합니다.
    기존 테이블이 있지만 UNIQUE 제약조건이 없으면 제약조건을 추가합니다.

    Args:
        engine: SQLAlchemy 엔진

    Returns:
        bool: 테이블이 새로 생성되었으면 True, 이미 존재하면 False
    """
    try:
        # 테이블 존재 여부 확인
        inspector = sa.inspect(engine)
        existing_tables = inspector.get_table_names()

        if "kind" in existing_tables:
            # 기존 테이블의 제약조건 확인
            constraints = inspector.get_unique_constraints("kind")
            has_unique_disclosure_id = any(
                "disclosure_id" in constraint["column_names"]
                for constraint in constraints
            )

            if has_unique_disclosure_id:
                logging.info(
                    "'kind' 테이블이 이미 존재하고 UNIQUE 제약조건이 있습니다."
                )
                return False
            else:
                logging.info(
                    "'kind' 테이블이 존재하지만 UNIQUE 제약조건이 없습니다. 제약조건을 추가합니다."
                )
                # 기존 테이블에 UNIQUE 제약조건 추가
                with engine.connect() as conn:
                    # 중복된 disclosure_id가 있는지 확인
                    duplicate_check = conn.execute(
                        sa.text(
                            """
                        SELECT disclosure_id, COUNT(*) 
                        FROM kind 
                        WHERE disclosure_id IS NOT NULL 
                        GROUP BY disclosure_id 
                        HAVING COUNT(*) > 1
                    """
                        )
                    ).fetchall()

                    if duplicate_check:
                        logging.warning(
                            f"중복된 disclosure_id가 {len(duplicate_check)}개 발견되었습니다. 중복 데이터를 제거합니다."
                        )
                        # 중복 데이터 제거 (가장 최근 데이터만 유지)
                        conn.execute(
                            sa.text(
                                """
                            DELETE FROM kind 
                            WHERE id NOT IN (
                                SELECT DISTINCT ON (disclosure_id) id 
                                FROM kind 
                                WHERE disclosure_id IS NOT NULL 
                                ORDER BY disclosure_id, created_at DESC
                            )
                        """
                            )
                        )
                        conn.commit()
                        logging.info("중복 데이터가 제거되었습니다.")

                    # UNIQUE 제약조건 추가
                    conn.execute(
                        sa.text(
                            "ALTER TABLE kind ADD CONSTRAINT uk_kind_disclosure_id UNIQUE (disclosure_id)"
                        )
                    )
                    conn.commit()
                    logging.info("UNIQUE 제약조건이 추가되었습니다.")
                    return False

        # 테이블 생성 SQL
        create_table_sql = """
        CREATE TABLE kind (
            id SERIAL PRIMARY KEY,
            disclosure_id VARCHAR(50) NOT NULL,
            disclosed_at TIMESTAMP,
            company_name VARCHAR(255),
            stock_code VARCHAR(20),
            short_code VARCHAR(20),
            market VARCHAR(20),
            title TEXT,
            summary_kr   TEXT,
            raw          TEXT,
            detail_url TEXT,
            is_modify INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uk_kind_disclosure_id UNIQUE (disclosure_id)
        );
        
        -- 인덱스 생성
        CREATE INDEX idx_kind_disclosed_at ON kind(disclosed_at);
        CREATE INDEX idx_kind_stock_code ON kind(stock_code);
        CREATE INDEX idx_kind_short_code ON kind(short_code);
        CREATE INDEX idx_kind_disclosure_id ON kind(disclosure_id);
        CREATE INDEX idx_kind_market ON kind(market);
        
        -- updated_at 자동 업데이트를 위한 트리거 함수
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        -- 트리거 생성
        CREATE TRIGGER update_kind_updated_at 
            BEFORE UPDATE ON kind 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """

        with engine.connect() as conn:
            conn.execute(sa.text(create_table_sql))
            conn.commit()

        logging.info("'kind' 테이블이 성공적으로 생성되었습니다.")
        return True

    except Exception as e:
        logging.error(f"'kind' 테이블 생성 중 오류 발생: {str(e)}")
        raise


def _insert_rows_to_kind_table(engine, rows: List[dict]) -> None:
    """
    추출된 rows를 kind 테이블에 삽입합니다.

    Args:
        engine: SQLAlchemy 엔진
        rows: 삽입할 데이터 리스트
    """
    try:

        # 각 row를 데이터베이스에 삽입
        for row in rows:
            # disclosure_id가 비어있으면 건너뛰기
            disclosure_id = row.get("disclosure_id", "")
            if not disclosure_id:
                logging.warning(f"disclosure_id가 비어있는 row를 건너뜁니다: {row}")
                continue

            # CSV에 없는 필드는 빈 문자열로 설정
            insert_sql = text(
                """
                INSERT INTO kind (
                    disclosure_id, disclosed_at, company_name, stock_code, short_code,
                    market, title, summary_kr, raw, detail_url, is_modify
                ) VALUES (
                    :disclosure_id, :disclosed_at, :company_name, :stock_code, :short_code,
                    :market, :title, :summary_kr, :raw, :detail_url, :is_modify
                )
                ON CONFLICT (disclosure_id) DO UPDATE SET
                    disclosed_at = EXCLUDED.disclosed_at,
                    company_name = EXCLUDED.company_name,
                    stock_code = EXCLUDED.stock_code,
                    short_code = EXCLUDED.short_code,
                    market = EXCLUDED.market,
                    title = EXCLUDED.title,
                    summary_kr = EXCLUDED.summary_kr,
                    raw = EXCLUDED.raw,
                    detail_url = EXCLUDED.detail_url,
                    is_modify = EXCLUDED.is_modify,
                    updated_at = CURRENT_TIMESTAMP
"""
            )

            # 데이터 준비 (없는 필드는 빈 문자열로)
            # disclosed_at를 TIMESTAMP 형식으로 변환
            disclosed_at = row.get("disclosed_at", "")
            if disclosed_at:
                try:
                    # "2022-06-30 18:48" 형식을 파싱
                    parsed_date = datetime.strptime(disclosed_at, "%Y-%m-%d %H:%M")
                    disclosed_at = parsed_date
                except ValueError:
                    # 파싱 실패 시 원본 값 유지
                    pass

            data = {
                "disclosure_id": disclosure_id,
                "disclosed_at": disclosed_at,
                "company_name": row.get("company_name", ""),
                "stock_code": row.get("stock_code", ""),
                "short_code": row.get("short_code", ""),
                "market": row.get("market", ""),
                "title": row.get("title", ""),
                "summary_kr": "",  # CSV에 없는 필드
                "raw": "",  # CSV에 없는 필드
                "detail_url": row.get("detail_url", ""),
                "is_modify": row.get("is_modify", 0),  # 정정 공시 여부 (기본값: 0)
            }

            with engine.connect() as conn:
                conn.execute(insert_sql, data)
                conn.commit()

    except Exception as e:
        logging.error(f"데이터베이스 삽입 중 오류 발생: {str(e)}")
        raise


def get_stock_code_by_company_name(company_name: str):
    """
    company_name을 받으면 stock_code를 반환합니다.
    """
    try:
        short_code = KRX_LISTING[KRX_LISTING["Name"] == company_name]["Code"].values[0]
        stock_code = KRX_LISTING[KRX_LISTING["Name"] == company_name]["ISU_CD"].values[
            0
        ]
    except Exception as e:
        # 에러 발생 시 None 반환
        stock_code = None
        short_code = None

    return stock_code, short_code


def create_dart_unique_number_table_if_not_exists(engine: sa.engine.Engine) -> bool:
    """
    DART 고유 번호 테이블이 데이터베이스에 없으면 생성합니다.
    """
    try:
        inspector = sa.inspect(engine)
        existing_tables = inspector.get_table_names()
        if "dart_unique_number" in existing_tables:
            logging.info("'dart_unique_number' 테이블이 이미 존재합니다.")
            return False
        else:
            create_table_sql = """
            CREATE TABLE dart_unique_number (
                id SERIAL PRIMARY KEY,
                dart_disclosure_id VARCHAR(20) UNIQUE,
                corp_name VARCHAR(255),
                corp_eng_name VARCHAR(255),
                stock_code VARCHAR(20),
                modify_date VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- 인덱스 생성
            CREATE INDEX idx_dart_unique_number_dart_disclosure_id ON dart_unique_number(dart_disclosure_id);
            CREATE INDEX idx_dart_unique_number_stock_code ON dart_unique_number(stock_code);
            CREATE INDEX idx_dart_unique_number_corp_name ON dart_unique_number(corp_name);
            
            -- updated_at 자동 업데이트를 위한 트리거 함수
            CREATE OR REPLACE FUNCTION update_dart_unique_number_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql';
            
            -- 트리거 생성
            CREATE TRIGGER update_dart_unique_number_updated_at 
                BEFORE UPDATE ON dart_unique_number 
                FOR EACH ROW 
                EXECUTE FUNCTION update_dart_unique_number_updated_at_column();
            """

            with engine.connect() as conn:
                conn.execute(sa.text(create_table_sql))
                conn.commit()

            logging.info("'dart_unique_number' 테이블이 성공적으로 생성되었습니다.")
            return True
    except Exception as e:
        logging.error(f"DART 고유 번호 테이블 생성 중 오류 발생: {str(e)}")
        raise


def save_into_dart_unique_number_table(
    api_key: str,
    engine: sa.engine.Engine,
    *,
    timeout: int = 30,
) -> None:
    """
    DART corpCode.xml(zip) 내려받아 XML 파싱 → DataFrame 반환.
    옵션으로 ZIP/내부 XML/CSV 저장 가능.
    """
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": api_key}

    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    content = resp.content

    # 먼저 ZIP 시도
    try:
        zip_bytes = io.BytesIO(content)
        with zipfile.ZipFile(zip_bytes) as zf:
            xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            if not xml_names:
                raise ValueError("ZIP 내부에 XML 파일이 없습니다.")

            with zf.open(xml_names[0]) as xf:
                xml_bytes = xf.read()
    except zipfile.BadZipFile:
        # ZIP이 아니면 보통 인증키 오류 등 에러 응답
        text = content.decode("utf-8", errors="ignore")
        raise ValueError(
            f"ZIP이 아닌 응답입니다. 인증키/쿼리를 확인하세요. 응답 예시: {text[:300]}"
        )

    # XML 파싱 → DataFrame
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        xml_text = xml_bytes.decode("utf-8", errors="ignore")
        root = ET.fromstring(xml_text)

    status = _find_text(root, ".//status")
    message = _find_text(root, ".//message")
    if status and status != "000":
        raise ValueError(f"DART API 오류(status={status}): {message}")

    corps = root.findall(".//list")
    rows = []
    for c in corps:
        short_code = _find_text(c, "stock_code")
        if short_code:
            rows.append(
                {
                    "dart_disclosure_id": _find_text(c, "corp_code"),
                    "corp_name": _find_text(c, "corp_name"),
                    "corp_eng_name": _find_text(c, "corp_eng_name"),
                    "stock_code": short_code,
                    "modify_date": _find_text(c, "modify_date"),
                }
            )

    df = pd.DataFrame(
        rows,
        columns=[
            "dart_disclosure_id",
            "corp_name",
            "corp_eng_name",
            "stock_code",
            "modify_date",
        ],
    )
    # DataFrame을 dart_unique_number 테이블에 삽입
    try:
        df.to_sql(
            "dart_unique_number",
            engine,
            if_exists="replace",
            index=False,
            method="multi",
        )
        logging.info(
            f"Successfully inserted {len(df)} rows into dart_unique_number table"
        )
    except Exception as e:
        logging.error(f"Failed to insert data into dart_unique_number table: {str(e)}")
        raise


def _find_text(root: ET.Element, path: str) -> str:
    el = root.find(path)
    return el.text.strip() if (el is not None and el.text is not None) else ""
