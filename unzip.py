import shutil
import gzip
import os
from pathlib import Path
from tqdm import tqdm


def fast_unzip():
    # 경로 설정
    hdd_gz = Path(
        "/Volumes/APFS/KOSPI/zip/SKSNXTRDIJH_2021_01.dat.gz"
    )  # HDD에 있는 압축 파일
    ssd_tmp = Path.home() / "Downloads"  # SSD 작업 폴더
    hdd_out = Path("/Volumes/APFS/KOSPI/unzip")  # HDD 최종 출력 폴더
    # HDD → SSD 복사
    print("Copying gz file to SSD...")
    ssd_gz = ssd_tmp / hdd_gz.name

    # 복사 진행률 표시
    source_size = os.path.getsize(hdd_gz)
    with tqdm(total=source_size, unit="B", unit_scale=True, desc="Copying") as pbar:
        with open(hdd_gz, "rb") as src, open(ssd_gz, "wb") as dst:
            while True:
                chunk = src.read(8192)  # 8KB 청크로 읽기
                if not chunk:
                    break
                dst.write(chunk)
                pbar.update(len(chunk))

    # 압축 해제 (입력=SSD, 출력=HDD)
    print("Extracting on SSD, saving to HDD...")
    output_file = hdd_out / hdd_gz.stem  # .gz 제거한 파일명

    # 파일 크기 가져오기 (진행률 표시용)
    file_size = os.path.getsize(ssd_gz)

    with gzip.open(ssd_gz, "rb") as f_in:
        with open(output_file, "wb") as f_out:
            # tqdm으로 진행률 표시
            with tqdm(
                total=file_size, unit="B", unit_scale=True, desc="Extracting"
            ) as pbar:
                while True:
                    chunk = f_in.read(8192)  # 8KB 청크로 읽기
                    if not chunk:
                        break
                    f_out.write(chunk)
                    pbar.update(len(chunk))

    print("Done!")

    # SSD 임시 gz 파일 삭제
    ssd_gz.unlink()


if __name__ == "__main__":
    fast_unzip()
