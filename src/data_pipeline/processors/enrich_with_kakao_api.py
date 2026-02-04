"""
카카오맵 API를 이용한 기업 데이터 보강

1. 좌표 결측치 보정: lat/lon이 NaN인 경우 주소로 좌표 검색
2. 법정동 코드 보강: 법정동 코드가 누락된 경우 좌표로 법정동 검색
3. 주소 중복 제거: 시도/시군구/읍면동 명칭에서 중복 제거
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import requests
import time
import urllib3
from config.settings import PathConfig, OutputConfig, ProcessConfig, APIConfig
from data_pipeline.api_clients.kakao_api import KakaoAPIClient
from core.utils import remove_address_duplicates

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 카카오 API 설정
KAKAO_HEADERS = {"Authorization": f"KakaoAK {APIConfig.KAKAO_API_KEY}"}
COORD2REGION_URL = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"


def get_coordinates_from_address(address):
    """
    주소를 좌표로 변환

    Args:
        address: 주소 문자열

    Returns:
        (latitude, longitude) 튜플 또는 (None, None)
    """
    if pd.isna(address) or not address.strip():
        return None, None

    kakao_client = KakaoAPIClient()
    lon, lat = kakao_client.get_coordinates_from_address(address)

    return lat, lon


def get_legal_dong_from_coord(lon, lat):
    """
    위경도를 법정동 코드로 변환

    Returns:
        dict: {
            'ctp_cd': 시도코드,
            'ctp_nm': 시도명,
            'sig_cd': 시군구코드,
            'sig_nm': 시군구명,
            'emd_cd': 읍면동코드,
            'emd_nm': 읍면동명
        }
    """
    try:
        params = {
            "x": lon,
            "y": lat,
            "input_coord": "WGS84"
        }

        response = requests.get(COORD2REGION_URL, headers=KAKAO_HEADERS, params=params, verify=False)

        if response.status_code != 200:
            return None

        data = response.json()

        if not data.get('documents'):
            return None

        # B (법정동) 타입 찾기
        legal_dong = None
        for doc in data['documents']:
            if doc.get('region_type') == 'B':
                legal_dong = doc
                break

        if not legal_dong:
            return None

        # 법정동 코드 파싱
        code = legal_dong.get('code', '')

        # 10자리 법정동 코드를 시도/시군구/읍면동으로 분리
        ctp_cd = code[:2] + '00000000' if len(code) >= 2 else None
        sig_cd = code[:5] + '00000' if len(code) >= 5 else None
        emd_cd = code if len(code) == 10 else None

        result = {
            'ctp_cd': ctp_cd,
            'ctp_nm': legal_dong.get('region_1depth_name', ''),
            'sig_cd': sig_cd,
            'sig_nm': f"{legal_dong.get('region_1depth_name', '')} {legal_dong.get('region_2depth_name', '')}".strip(),
            'emd_cd': emd_cd,
            'emd_nm': f"{legal_dong.get('region_1depth_name', '')} {legal_dong.get('region_2depth_name', '')} {legal_dong.get('region_3depth_name', '')}".strip()
        }

        return result

    except Exception:
        return None


def fix_missing_coordinates(df):
    """
    좌표 결측치 보정

    Args:
        df: 기업 데이터프레임

    Returns:
        보정된 데이터프레임
    """
    print("=" * 70)
    print("[1/3] 좌표 결측치 보정")
    print("=" * 70)
    print()

    # 좌표가 누락된 행 찾기
    missing_coords_mask = (df['lat'].isna() | df['lon'].isna())
    missing_count = missing_coords_mask.sum()

    print(f"[INFO] 좌표 누락: {missing_count:,}개 ({missing_count / len(df) * 100:.1f}%)")
    print()

    if missing_count == 0:
        print("[OK] 모든 기업에 좌표가 있습니다. 보정 불필요.")
        print()
        return df

    # 누락된 행에 대해 API 호출
    enriched_count = 0
    failed_count = 0
    total_missing = missing_count

    for idx in df[missing_coords_mask].index:
        address = df.loc[idx, 'all_addr_nm']

        progress = enriched_count + failed_count + 1
        if progress % 100 == 0 or progress == 1 or progress == total_missing:
            print(f"진행: {progress:,}/{total_missing:,} - 성공: {enriched_count:,}, 실패: {failed_count:,}")

        if pd.isna(address) or not address.strip():
            failed_count += 1
            continue

        lat, lon = get_coordinates_from_address(address)

        if lat is not None and lon is not None:
            df.at[idx, 'lat'] = lat
            df.at[idx, 'lon'] = lon
            enriched_count += 1
        else:
            failed_count += 1

        # API 호출 제한
        if ProcessConfig.API_REQUEST_DELAY > 0:
            time.sleep(ProcessConfig.API_REQUEST_DELAY)

    print()
    print(f"[OK] 좌표 보정 완료: 성공 {enriched_count:,}개, 실패 {failed_count:,}개")
    print()

    return df


def enrich_legal_dong_codes(df):
    """
    기업 데이터의 누락된 법정동 코드 보강

    Args:
        df: 기업 데이터프레임

    Returns:
        보정된 데이터프레임
    """
    print("=" * 70)
    print("[2/3] 법정동 코드 보강")
    print("=" * 70)
    print()

    # 법정동 코드가 누락된 행 찾기
    missing_mask = (
        df['ctp_cd'].isna() |
        df['sig_cd'].isna() |
        df['emd_cd'].isna()
    )
    missing_count = missing_mask.sum()

    print(f"[INFO] 법정동 코드 누락: {missing_count:,}개 ({missing_count / len(df) * 100:.1f}%)")
    print()

    if missing_count == 0:
        print("[OK] 모든 기업에 법정동 코드가 있습니다. 보강 불필요.")
        print()
        return df

    # 누락된 행에 대해 API 호출
    enriched_count = 0
    failed_count = 0
    total_missing = missing_count

    for idx in df[missing_mask].index:
        lat = df.loc[idx, 'lat']
        lon = df.loc[idx, 'lon']

        progress = enriched_count + failed_count + 1
        if progress % 100 == 0 or progress == 1 or progress == total_missing:
            print(f"진행: {progress:,}/{total_missing:,} - 성공: {enriched_count:,}, 실패: {failed_count:,}")

        if pd.isna(lat) or pd.isna(lon):
            failed_count += 1
            continue

        legal_dong = get_legal_dong_from_coord(lon, lat)

        if legal_dong:
            df.at[idx, 'ctp_cd'] = legal_dong['ctp_cd']
            df.at[idx, 'ctp_nm'] = legal_dong['ctp_nm']
            df.at[idx, 'sig_cd'] = legal_dong['sig_cd']
            df.at[idx, 'sig_nm'] = legal_dong['sig_nm']
            df.at[idx, 'emd_cd'] = legal_dong['emd_cd']
            df.at[idx, 'emd_nm'] = legal_dong['emd_nm']
            enriched_count += 1
        else:
            failed_count += 1

        # API 호출 제한
        if ProcessConfig.API_REQUEST_DELAY > 0:
            time.sleep(ProcessConfig.API_REQUEST_DELAY)

    print()
    print(f"[OK] 법정동 코드 보강 완료: 성공 {enriched_count:,}개, 실패 {failed_count:,}개")
    print()

    return df


def apply_address_deduplication(df):
    """
    주소 중복 제거 적용

    Args:
        df: 기업 데이터프레임

    Returns:
        중복 제거된 데이터프레임
    """
    print("=" * 70)
    print("[3/3] 주소 중복 제거")
    print("=" * 70)
    print()

    cleaned_count = 0
    total = len(df)

    for idx, row in df.iterrows():
        if (idx + 1) % 500 == 0 or idx == 0 or (idx + 1) == total:
            print(f"진행 중: {idx + 1}/{total} ({(idx + 1) / total * 100:.1f}%)")

        ctp_nm_clean, sig_nm_clean, emd_nm_clean = remove_address_duplicates(
            row['ctp_nm'], row['sig_nm'], row['emd_nm']
        )

        # 변경 여부 확인
        if (ctp_nm_clean != row['ctp_nm'] or
            sig_nm_clean != row['sig_nm'] or
            emd_nm_clean != row['emd_nm']):
            cleaned_count += 1

        df.at[idx, 'ctp_nm'] = ctp_nm_clean
        df.at[idx, 'sig_nm'] = sig_nm_clean
        df.at[idx, 'emd_nm'] = emd_nm_clean

    print()
    print(f"[OK] 주소 중복 제거: {cleaned_count}개 ({cleaned_count / total * 100:.1f}%)")
    print()

    return df


def enrich_company_data():
    """
    기업 데이터 보강 (좌표 결측치 + 법정동 코드 + 주소 중복 제거)
    """
    start_time = time.time()

    print("기업 데이터 보강 시작")
    print()

    try:
        # 1. 데이터 로드 (final에서 최신 파일)
        from core.utils import ReferenceDataManager
        ref_manager = ReferenceDataManager("기업위치")

        input_path = ref_manager.get_latest_csv_file()
        if not input_path or not input_path.exists():
            raise FileNotFoundError(f"기업위치 파일을 찾을 수 없습니다: {PathConfig.FINAL_DATA_DIR}")

        df = pd.read_csv(input_path, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"[OK] 기업 데이터 로드: {len(df):,}개 ({input_path.name})")
        print()

        # 2. 좌표 결측치 보정
        df = fix_missing_coordinates(df)

        # 3. 법정동 코드 보강
        df = enrich_legal_dong_codes(df)

        # 4. 주소 중복 제거
        df = apply_address_deduplication(df)

        # 5. 숫자 코드형 컬럼을 정수형 문자열로 변환 (corp_depth1_cd는 문자 포함되어 제외)
        code_columns = ['corp_cd', 'ctp_cd', 'sig_cd', 'emd_cd',
                        'corp_depth2_cd', 'corp_depth3_cd',
                        'corp_depth4_cd', 'corp_depth5_cd']
        for col in code_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) and x != '' else '')

        # 6. 위경도 소수점 10자리로 제한
        for col in ['lat', 'lon']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: round(x, 10) if pd.notna(x) else x)

        # 7. 저장 (같은 파일에 덮어쓰기)
        df.to_csv(input_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"[OK] 데이터 저장: {input_path.name}")
        print()

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print("=" * 70)
        print("작업 완료!")
        print("=" * 70)
        print(f"저장 파일: {input_path}")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("=" * 70)
        print()

    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        raise


def main():
    """
    메인 실행 함수
    """
    print("=" * 70)
    print("기업 데이터 보강 (카카오맵 API)")
    print("=" * 70)
    print()

    enrich_company_data()


if __name__ == "__main__":
    main()
