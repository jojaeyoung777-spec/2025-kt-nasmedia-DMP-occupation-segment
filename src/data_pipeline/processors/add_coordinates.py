"""
카카오 로컬 API를 사용하여 주소에 좌표 정보 및 법정동코드 추가

주소 → 좌표 변환 후, 좌표 → 법정동코드 변환을 통해
ctp_cd, ctp_nm, sig_cd, sig_nm, emd_cd, emd_nm 정보를 함께 추가합니다.
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import PathConfig, OutputConfig, ProcessConfig
from data_pipeline.api_clients.kakao_api import KakaoAPIClient
from core.utils import clean_address_for_search

def process_single_address(idx, address):
    """
    단일 주소를 좌표 및 법정동 정보로 변환 (병렬 처리용)

    재시도 로직:
    1차 시도: 원본 주소로 검색
    2차 시도: 층/호/지하/괄호 제거 후 재검색

    Args:
        idx: 데이터프레임의 인덱스
        address: 검색할 주소

    Returns:
        (idx, longitude, latitude, legal_dong_info) 튜플
    """
    kakao_client = KakaoAPIClient()

    # 1차 시도: 원본 주소로 검색
    longitude, latitude = kakao_client.get_coordinates_from_address(address)

    # 2차 시도: 1차 실패 시 정제된 주소로 재검색
    if longitude is None and latitude is None:
        cleaned_address = clean_address_for_search(address)

        # 정제된 주소가 원본과 다른 경우에만 재시도
        if cleaned_address != address and cleaned_address:
            longitude, latitude = kakao_client.get_coordinates_from_address(cleaned_address)

    # 2. 좌표 → 법정동 변환
    legal_dong_info = None
    if longitude is not None and latitude is not None:
        legal_dong_info = kakao_client.get_legal_dong_from_coord(longitude, latitude)

    # Rate limiting
    if ProcessConfig.API_REQUEST_DELAY > 0:
        time.sleep(ProcessConfig.API_REQUEST_DELAY)

    return idx, longitude, latitude, legal_dong_info

def add_coordinates_to_dataframe(df):
    """
    데이터프레임에 좌표 정보 및 법정동 정보 추가 (안정화된 병렬 처리)

    Args:
        df: DART 기업 데이터프레임 (adres 컬럼 필수)

    Returns:
        좌표 및 법정동 정보가 추가된 데이터프레임
    """
    print("=" * 70)
    print("카카오 API로 좌표 및 법정동 정보 추가 (안정화된 병렬 처리)")
    print("=" * 70)
    print(f"총 {len(df)}개 기업의 주소를 좌표 및 법정동으로 변환합니다...")
    print(f"병렬 처리: {ProcessConfig.MAX_WORKERS}개 스레드 동시 실행")
    print(f"재시도: {ProcessConfig.API_RETRY_COUNT}회, 딜레이: {ProcessConfig.API_REQUEST_DELAY}초")
    print()

    # 주소가 있는 기업만 카운트
    valid_addresses = df['adres'].notna() & (df['adres'].str.strip() != '')
    print(f"유효한 주소: {valid_addresses.sum()}개")
    print()

    # 결과를 인덱스별로 저장 (순서 보장)
    results = {}
    total = len(df)
    coord_success_count = 0
    legal_dong_success_count = 0
    completed = 0

    # 병렬 처리
    with ThreadPoolExecutor(max_workers=ProcessConfig.MAX_WORKERS) as executor:
        # 모든 작업 제출 (인덱스와 함께)
        futures = {
            executor.submit(process_single_address, idx, row['adres']): idx
            for idx, row in df.iterrows()
        }

        # 완료된 작업 처리
        for future in as_completed(futures):
            completed += 1
            if completed % 100 == 0 or completed == 1 or completed == total:
                print(f"진행 중: {completed}/{total} ({completed / total * 100:.1f}%) - 좌표: {coord_success_count}개, 법정동: {legal_dong_success_count}개")

            try:
                idx, longitude, latitude, legal_dong_info = future.result(timeout=ProcessConfig.API_TIMEOUT + 10)

                # 좌표 정보
                results[idx] = {
                    'longitude': longitude,
                    'latitude': latitude,
                    'ctp_cd': None,
                    'ctp_nm': None,
                    'sig_cd': None,
                    'sig_nm': None,
                    'emd_cd': None,
                    'emd_nm': None
                }

                if longitude is not None:
                    coord_success_count += 1

                # 법정동 정보
                if legal_dong_info:
                    results[idx]['ctp_cd'] = legal_dong_info.get('ctp_cd')
                    results[idx]['ctp_nm'] = legal_dong_info.get('ctp_nm')
                    results[idx]['sig_cd'] = legal_dong_info.get('sig_cd')
                    results[idx]['sig_nm'] = legal_dong_info.get('sig_nm')
                    results[idx]['emd_cd'] = legal_dong_info.get('emd_cd')
                    results[idx]['emd_nm'] = legal_dong_info.get('emd_nm')
                    legal_dong_success_count += 1

            except Exception as e:
                # 실패한 작업의 인덱스 찾기
                idx = futures[future]
                results[idx] = {
                    'longitude': None,
                    'latitude': None,
                    'ctp_cd': None,
                    'ctp_nm': None,
                    'sig_cd': None,
                    'sig_nm': None,
                    'emd_cd': None,
                    'emd_nm': None
                }

    # 인덱스 순서대로 결과 정렬하여 데이터프레임에 추가
    sorted_results = [results[idx] for idx in sorted(results.keys())]
    df['longitude'] = [r['longitude'] for r in sorted_results]
    df['latitude'] = [r['latitude'] for r in sorted_results]
    df['ctp_cd'] = [r['ctp_cd'] for r in sorted_results]
    df['ctp_nm'] = [r['ctp_nm'] for r in sorted_results]
    df['sig_cd'] = [r['sig_cd'] for r in sorted_results]
    df['sig_nm'] = [r['sig_nm'] for r in sorted_results]
    df['emd_cd'] = [r['emd_cd'] for r in sorted_results]
    df['emd_nm'] = [r['emd_nm'] for r in sorted_results]

    print(f"\n[OK] 완료!")
    print(f"좌표 변환 성공: {coord_success_count}개 ({coord_success_count / total * 100:.1f}%)")
    print(f"법정동 변환 성공: {legal_dong_success_count}개 ({legal_dong_success_count / total * 100:.1f}%)")
    print()

    return df

def main():
    """
    메인 실행 함수
    """
    start_time = time.time()

    try:
        # 1. Raw 데이터 로드
        print("=" * 70)
        print("[1/2] DART 기업 데이터 로드")
        print("=" * 70)

        # 최신 raw 파일 찾기 (날짜 포함 파일명)
        pattern = "기업위치_raw_*.csv"
        raw_files = list(PathConfig.RAW_DATA_DIR.glob(pattern))

        if not raw_files:
            # Fallback: 고정 파일명 시도
            input_path = PathConfig.RAW_DATA_DIR / OutputConfig.RAW_DART_COMPANIES
            if not input_path.exists():
                raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {PathConfig.RAW_DATA_DIR}")
        else:
            # 날짜별로 정렬 (최신 파일 선택)
            raw_files.sort(key=lambda x: x.stem.split('_')[-1], reverse=True)
            input_path = raw_files[0]

        print(f"[INFO] 사용할 파일: {input_path.name}")

        df = pd.read_csv(input_path, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"[OK] 총 {len(df)}개 기업 로드")
        print(f"[INFO] 컬럼: {', '.join(df.columns.tolist())}")
        print()

        # 주소 컬럼 확인 (adres 컬럼이 raw에 이미 있음)
        if 'adres' not in df.columns:
            raise ValueError("adres 컬럼이 없습니다. 파일 구조를 확인하세요.")

        # 2. 좌표 추가 (무조건 API 호출)
        df_with_coords = add_coordinates_to_dataframe(df)

        # 3. 저장
        output_path = PathConfig.INTERMEDIATE_DATA_DIR / OutputConfig.INTERMEDIATE_WITH_COORDINATES
        df_with_coords.to_csv(output_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print("=" * 70)
        print("작업 완료!")
        print("=" * 70)
        print(f"저장 파일: {output_path}")
        print(f"총 기업 수: {len(df_with_coords)}개")
        print(f"컬럼 수: {len(df_with_coords.columns)}개")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("=" * 70)

        # 샘플 데이터 출력
        print("\n좌표 및 법정동이 추가된 샘플 데이터 (첫 5개):")
        sample_cols = ['corp_name', 'adres', 'longitude', 'latitude', 'ctp_nm', 'sig_nm', 'emd_nm']
        print(df_with_coords[sample_cols].head(5).to_string())
        print()

    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        raise

if __name__ == "__main__":
    main()
