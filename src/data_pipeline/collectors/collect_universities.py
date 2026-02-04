"""
전국 대학교 데이터 수집 (날짜별 버전 관리)
안전지도 API IF_0034 (대학교) 사용

API로 받은 데이터를 날짜별 CSV로 저장하고, 실패 시 가장 최근 CSV를 fallback으로 사용
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
import urllib3
from datetime import datetime
from pyproj import Transformer
from config.settings import APIConfig, PathConfig, OutputConfig, ProcessConfig
from core.utils import ReferenceDataManager

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 좌표 변환기 생성 (Web Mercator -> WGS84)
transformer = Transformer.from_crs(
    ProcessConfig.COORD_SOURCE_CRS,
    ProcessConfig.COORD_TARGET_CRS,
    always_xy=True
)


def collect_universities():
    """
    안전지도 API를 통해 전국 대학교 데이터 수집
    API: IF_0034 (대학교)

    Returns:
        DataFrame: 대학교 데이터 (시설코드, 시설명, 주소, 좌표)
    """
    print("=" * 70)
    print("[1/1] 대학교 데이터 수집 (API: IF_0034)")
    print("=" * 70)
    print()

    all_universities = []
    page = 1
    retry_count = 0
    MAX_RETRIES = 2  # 최대 2회 재시도 (총 3번 시도)
    fac_cd_counter = 502040  # fac_cd 시작 값

    print(f"API 엔드포인트: {APIConfig.SAFEMAP_UNIVERSITY_URL}")
    print(f"인증키: {APIConfig.SAFEMAP_SERVICE_KEY}")
    print(f"페이지 수 제한: 없음 (전체 수집)")
    print(f"재시도 제한: 최대 {MAX_RETRIES}회")
    print()

    while True:
        params = {
            'serviceKey': APIConfig.SAFEMAP_SERVICE_KEY,
            'pageNo': str(page),
            'numOfRows': '100',
            'returnType': 'XML'
        }

        try:
            response = requests.get(
                APIConfig.SAFEMAP_UNIVERSITY_URL,
                params=params,
                timeout=30,
                verify=False
            )

            if response.status_code != 200:
                print(f"페이지 {page}: HTTP {response.status_code} - 중단")
                break

            # XML 파싱
            try:
                root = ET.fromstring(response.content)
            except ET.ParseError as e:
                print(f"페이지 {page}: XML 파싱 오류 - {str(e)[:50]}... - 스킵")
                page += 1
                continue

            # 결과 코드 확인
            result_code = root.find('.//resultCode')
            if result_code is not None and result_code.text != '00':
                result_msg = root.find('.//resultMsg')
                print(f"API 오류: {result_msg.text if result_msg is not None else '알 수 없는 오류'}")
                break

            # 데이터 추출
            items = root.findall('.//item')

            if not items:
                print(f"페이지 {page}: 데이터 없음 - 완료")
                break

            for item in items:
                fclty_nm = item.find('fclty_nm')
                adres = item.find('adres')
                rn_adres = item.find('rn_adres')

                # 좌표
                x = item.find('x')
                y = item.find('y')

                # 주소 정보 (도로명주소 우선, 없으면 지번주소)
                addr_text = ''
                if rn_adres is not None and rn_adres.text:
                    addr_text = rn_adres.text
                elif adres is not None and adres.text:
                    addr_text = adres.text

                # 좌표 변환 (Web Mercator -> WGS84)
                lat_val = ''
                lon_val = ''
                if x is not None and x.text and y is not None and y.text:
                    try:
                        x_val = float(x.text)
                        y_val = float(y.text)
                        lon_converted, lat_converted = transformer.transform(x_val, y_val)
                        lat_val = str(lat_converted)
                        lon_val = str(lon_converted)
                    except (ValueError, Exception):
                        pass

                all_universities.append({
                    'fac_cd': str(fac_cd_counter),
                    'fac_nm': fclty_nm.text if fclty_nm is not None and fclty_nm.text else '',
                    'all_addr_nm': addr_text,
                    'lat': lat_val,
                    'lon': lon_val
                })
                fac_cd_counter += 1

            if (page % 10 == 0) or (page == 1):
                print(f"페이지 {page:3d} 완료 - 누적: {len(all_universities):4d}개")

            # 마지막 페이지 확인
            if len(items) < 100:
                print(f"페이지 {page}: 마지막 페이지 - 완료")
                break

            # 성공 시 재시도 카운터 리셋
            retry_count = 0
            page += 1

        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"페이지 {page} 네트워크 오류 ({retry_count}/{MAX_RETRIES}회): {str(e)[:50]}...")

            if retry_count > MAX_RETRIES:
                print(f"[ERROR] 최대 재시도 횟수 초과 - Fallback으로 전환")
                raise Exception(f"SafeMap API 연결 실패 (재시도 {MAX_RETRIES}회 초과)")

            time.sleep(1)  # 재시도 전 1초 대기
            continue

        except Exception as e:
            print(f"페이지 {page} 예상치 못한 오류: {str(e)[:50]}... - 중단")
            break

    df = pd.DataFrame(all_universities) if all_universities else pd.DataFrame(
        columns=['fac_cd', 'fac_nm', 'all_addr_nm', 'lat', 'lon']
    )

    print()
    print(f"[OK] 총 {len(df)}개 대학교 수집 완료")
    if len(df) > 0:
        print(f"[OK] 주소 제공률: {df['all_addr_nm'].notna().sum()}개 ({df['all_addr_nm'].notna().sum()/len(df)*100:.1f}%)")
    print()

    return df


def cleanup_old_raw_files(data_name: str, keep_file: Path):
    """
    이전 날짜의 raw 파일들 삭제 (최신 파일만 유지)

    Args:
        data_name: 데이터 이름 (예: "대학교")
        keep_file: 유지할 파일 경로
    """
    pattern = f"{data_name}_raw_*.csv"
    old_files = list(PathConfig.RAW_DATA_DIR.glob(pattern))
    deleted_count = 0

    for file_path in old_files:
        if file_path != keep_file:
            try:
                file_path.unlink()
                deleted_count += 1
            except Exception:
                pass

    if deleted_count > 0:
        print(f"[OK] 이전 raw 파일 {deleted_count}개 삭제됨")


def main():
    """
    메인 실행 함수

    0. API로 데이터 수집
    1. raw 디렉토리에 저장
    1.1. API 실패 시 → 최신 raw 사용 (전처리는 processor가 담당)
    """
    start_time = time.time()

    # 날짜 포함 파일명 생성
    today = datetime.now().strftime('%Y%m%d')
    raw_filename = f"대학교_raw_{today}.csv"
    raw_path = PathConfig.RAW_DATA_DIR / raw_filename

    try:
        # 0. API에서 대학교 데이터 수집
        df = collect_universities()

        if len(df) == 0:
            raise Exception("수집된 데이터가 없습니다")

        # 1. Raw 폴더에 저장만 수행 (final은 processor가 생성)
        PathConfig.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(raw_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"[OK] Raw 데이터 저장: {raw_path.name} ({len(df)}개)")

        # 이전 날짜 파일들 삭제
        cleanup_old_raw_files("대학교", raw_path)

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print()
        print("=" * 70)
        print("대학교 데이터 수집 완료!")
        print("=" * 70)
        print(f"Raw 파일: {raw_path}")
        print(f"총 학교 수: {len(df)}개")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("[INFO] 전처리는 데이터 파이프라인에서 수행됩니다.")
        print("=" * 70)
        print()

    except Exception as e:
        print(f"\n[ERROR] API 호출 실패: {e}")

        # 1.1. Fallback: 최신 raw 파일 확인
        if ProcessConfig.USE_FALLBACK:
            print("[INFO] Fallback: API 호출 실패, raw/ 폴더의 기존 데이터를 사용합니다.")

            # 최신 Raw 파일 찾기
            pattern = "대학교_raw_*.csv"
            raw_files = list(PathConfig.RAW_DATA_DIR.glob(pattern))

            if raw_files:
                # 날짜별로 정렬 (최신 파일 선택)
                raw_files.sort(key=lambda x: x.stem.split('_')[-1], reverse=True)
                fallback_raw_path = raw_files[0]

                print(f"[INFO] 기존 Raw 파일 사용: {fallback_raw_path.name}")
                print("       추가 전처리는 데이터 파이프라인에서 수행됩니다.")
            else:
                print(f"[ERROR] Fallback용 Raw 파일을 찾을 수 없습니다")
        else:
            print("[ERROR] Fallback이 비활성화되어 있습니다.")


if __name__ == "__main__":
    main()
