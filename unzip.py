#!/usr/bin/env python3
"""
gunzip_dat.py  ── .dat.gz → .dat 일괄 압축 해제 스크립트

$ python gunzip_dat.py          # 기본 경로 사용
$ python gunzip_dat.py -h       # 도움말
"""

import argparse
import gzip
import shutil
from pathlib import Path


# ────────────────────────────────────────────────────────────────────
def decompress_dir(src_dir: Path, dst_dir: Path, overwrite: bool = False) -> None:
    """
    src_dir 안의 *.dat.gz 파일을 모두 풀어 dst_dir 에 .dat 로 저장.
    이미 존재하는 출력 파일은 기본적으로 건너뛰며, `overwrite=True`면 덮어쓴다.
    """
    if not src_dir.exists():
        raise FileNotFoundError(f"소스 디렉터리가 없습니다: {src_dir}")

    dst_dir.mkdir(parents=True, exist_ok=True)

    gz_files = sorted(src_dir.glob("*.dat.gz"))
    if not gz_files:
        print(f"[INFO] {src_dir} 에 .dat.gz 파일이 없습니다.")
        return

    for gz_path in gz_files:
        dat_path = dst_dir / gz_path.with_suffix("").name  # .gz 제거 → .dat
        if dat_path.exists() and not overwrite:
            print(f"[SKIP] {dat_path} (이미 존재)")
            continue

        try:
            with gzip.open(gz_path, "rb") as f_in, open(dat_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            print(f"[OK]   {gz_path.name}  →  {dat_path.relative_to(dst_dir)}")
        except Exception as e:
            print(f"[ERR]  {gz_path}: {e}")


# ────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description=".dat.gz 파일을 .dat 로 일괄 압축 해제",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--kospi-src",
        type=Path,
        default=Path("/Volumes/APFS/KOSPI/zip"),
        help="KOSPI .gz 소스 디렉터리",
    )
    parser.add_argument(
        "--kospi-dst",
        type=Path,
        default=Path("/Volumes/APFS/KOSPI/unzip"),
        help="KOSPI .dat 출력 디렉터리",
    )
    parser.add_argument(
        "--kosaq-src",
        type=Path,
        default=Path("/Volumes/APFS/KOSAQ/zip"),
        help="KOSAQ .gz 소스 디렉터리",
    )
    parser.add_argument(
        "--kosaq-dst",
        type=Path,
        default=Path("/Volumes/APFS/KOSAQ/unzip"),
        help="KOSAQ .dat 출력 디렉터리",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="이미 존재하는 .dat 파일을 덮어씀",
    )
    args = parser.parse_args()

    print("── KOSPI ─────────────────────────────────────────────")
    decompress_dir(args.kospi_src, args.kospi_dst, overwrite=args.force)

    print("\n── KOSAQ ─────────────────────────────────────────────")
    decompress_dir(args.kosaq_src, args.kosaq_dst, overwrite=args.force)


if __name__ == "__main__":
    main()
