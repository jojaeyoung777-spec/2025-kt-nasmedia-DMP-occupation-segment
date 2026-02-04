"""
법정동코드 매칭

주소 정보를 기반으로 법정동코드(시도, 시군구, 읍면동)를 매칭합니다.
** API 리뉴얼 버전 **
- 고등학교: 주소 기반 매칭 + 주소 중복 제거
- 대학교: 주소 기반 매칭 + 좌표로 카카오 API 보정 + 주소 중복 제거
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import time
from config.settings import PathConfig, OutputConfig, ProcessConfig
from data_pipeline.api_clients.kakao_api import KakaoAPIClient
from core.utils import remove_address_duplicates


def load_legal_dong_codes():
    """
    법정동코드 CSV에서 로드

    Returns:
        dict: {법정동명: 법정동코드} 딕셔너리
    """
    print("=" * 70)
    print("[1/3] 법정동코드 CSV 로드")
    print("=" * 70)
    print()

    # final 폴더에서 최신 법정동코드 CSV 찾기
    csv_files = list(PathConfig.FINAL_DATA_DIR.glob("법정동코드_*.csv"))
    if not csv_files:
        raise FileNotFoundError("법정동코드 CSV 파일을 찾을 수 없습니다. collect_legal_dong_codes.py를 먼저 실행하세요.")

    # 날짜별로 정렬 (최신 파일 선택)
    csv_files.sort(key=lambda x: x.stem.split('_')[1] if '_' in x.stem else '', reverse=True)
    latest_file = csv_files[0]

    print(f"[INFO] 로드할 파일: {latest_file.name}")

    # CSV 로드
    df = pd.read_csv(latest_file, encoding=ProcessConfig.ENCODING_DEFAULT)

    # 딕셔너리로 변환 {법정동명: 법정동코드}
    code_dict = {}
    for _, row in df.iterrows():
        code = str(row['법정동코드']).strip()
        name = str(row['법정동명']).strip()
        if code and name:
            code_dict[name] = code

    print(f"[OK] 법정동코드 딕셔너리 로드 완료: {len(code_dict)}개")
    print()

    return code_dict


def extract_region_from_address(address, code_dict):
    """
    주소에서 행정구역 정보 추출

    Args:
        address: 주소 문자열
        code_dict: 법정동코드 딕셔너리

    Returns:
        tuple: (ctp_cd, ctp_nm, sig_cd, sig_nm, emd_cd, emd_nm)
    """
    if pd.isna(address) or not address:
        return None, None, None, None, None, None

    address = str(address).strip()

    # 가장 긴 매칭부터 시도 (더 구체적인 행정구역 우선)
    best_match = None
    best_length = 0

    for legal_name, legal_code in code_dict.items():
        if legal_name in address and len(legal_name) > best_length:
            best_match = (legal_name, legal_code)
            best_length = len(legal_name)

    if best_match:
        legal_name, legal_code = best_match

        # 읍면동 코드 및 이름
        emd_cd = legal_code
        emd_nm = legal_name

        # 시군구 코드 (앞 5자리 + '00000')
        sig_cd = legal_code[:5] + '00000'

        # 시도 코드 (앞 2자리 + '00000000')
        ctp_cd = legal_code[:2] + '00000000'

        # 시군구명, 시도명 추출
        parts = legal_name.split()
        if len(parts) >= 1:
            ctp_nm = parts[0]
        else:
            ctp_nm = ''

        if len(parts) >= 2:
            sig_nm = parts[1] if len(parts) == 2 else ' '.join(parts[:2])
        else:
            sig_nm = ''

        return ctp_cd, ctp_nm, sig_cd, sig_nm, emd_cd, emd_nm

    return None, None, None, None, None, None


def enrich_with_kakao_coord(row, kakao_client):
    """
    좌표 기반으로 카카오 API를 조회하여 법정동 정보 보정

    Args:
        row: DataFrame 행 (lat, lon 포함)
        kakao_client: KakaoAPIClient 인스턴스

    Returns:
        dict: 보정된 법정동 정보
    """
    try:
        lat = row.get('lat')
        lon = row.get('lon')

        if pd.notna(lat) and pd.notna(lon):
            lat_val = float(lat)
            lon_val = float(lon)

            legal_dong_info = kakao_client.get_legal_dong_from_coord(lon_val, lat_val)

            if legal_dong_info:
                return legal_dong_info

    except Exception:
        pass

    return None


def match_legal_dong_codes(df, code_dict, is_university=False):
    """
    데이터프레임에 법정동코드 정보 추가

    Args:
        df: 학교 데이터프레임 (all_addr_nm 컬럼 필수)
        code_dict: 법정동코드 딕셔너리
        is_university: 대학교 여부 (True인 경우 좌표로 보정)

    Returns:
        DataFrame: 법정동코드가 추가된 데이터프레임
    """
    step_title = "[2/3] 법정동코드 매칭" if not is_university else "[2/4] 법정동코드 매칭 (주소 기반)"
    print("=" * 70)
    print(step_title)
    print("=" * 70)
    print()

    results = []
    total = len(df)
    matched_count = 0

    for idx, row in df.iterrows():
        if (idx + 1) % 200 == 0 or idx == 0 or (idx + 1) == total:
            print(f"진행 중: {idx + 1}/{total} ({(idx + 1) / total * 100:.1f}%) - 매칭: {matched_count}개")

        ctp_cd, ctp_nm, sig_cd, sig_nm, emd_cd, emd_nm = extract_region_from_address(
            row['all_addr_nm'], code_dict
        )

        if ctp_cd is not None:
            matched_count += 1

        results.append({
            'ctp_cd': ctp_cd,
            'ctp_nm': ctp_nm,
            'sig_cd': sig_cd,
            'sig_nm': sig_nm,
            'emd_cd': emd_cd,
            'emd_nm': emd_nm
        })

    print(f"\n[OK] 완료!")
    print(f"법정동코드 매칭: {matched_count}개 ({matched_count / total * 100:.1f}%)")
    print()

    return pd.DataFrame(results)




def apply_address_deduplication(region_df, step_label="[2/2] 주소 중복 제거"):
    """
    주소 중복 제거 적용

    Args:
        region_df: 법정동 정보 DataFrame
        step_label: 단계 레이블

    Returns:
        DataFrame: 중복 제거된 법정동 정보
    """
    print("=" * 70)
    print(step_label)
    print("=" * 70)
    print()

    cleaned_results = []
    total = len(region_df)
    cleaned_count = 0

    for idx, row in region_df.iterrows():
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

        cleaned_results.append({
            'ctp_cd': row['ctp_cd'],
            'ctp_nm': ctp_nm_clean,
            'sig_cd': row['sig_cd'],
            'sig_nm': sig_nm_clean,
            'emd_cd': row['emd_cd'],
            'emd_nm': emd_nm_clean
        })

    print(f"\n[OK] 완료!")
    print(f"주소 중복 제거: {cleaned_count}개 ({cleaned_count / total * 100:.1f}%)")
    print()

    return pd.DataFrame(cleaned_results)


def enrich_schools_with_reverse_geocoding(df, school_type="학교"):
    """
    학교 데이터에 대해 좌표 기반으로 법정동 정보 역지오코딩 (고등학교/대학교 공통)

    주소 기반 매칭 없이 오직 좌표만 사용하여 정확한 법정동 정보 추출
    카카오 API 역지오코딩으로 법정동 코드(emd_cd) 포함 전체 정보 갱신

    Args:
        df: 학교 원본 데이터 (lat, lon 필수)
        school_type: 학교 유형 (로깅용)

    Returns:
        DataFrame: 좌표 기반 법정동 정보 (법정동 코드 포함)
    """
    print("=" * 70)
    print(f"[1/2] 역지오코딩으로 법정동 정보 추출 ({school_type})")
    print("=" * 70)
    print()

    kakao_client = KakaoAPIClient()
    enriched_results = []
    enriched_count = 0
    total = len(df)

    for idx, row in df.iterrows():
        if (idx + 1) % 100 == 0 or idx == 0 or (idx + 1) == total:
            print(f"진행 중: {idx + 1}/{total} ({(idx + 1) / total * 100:.1f}%) - 성공: {enriched_count}개")

        # 좌표 기반 역지오코딩 (주소는 사용하지 않음)
        enriched_info = enrich_with_kakao_coord(row, kakao_client)

        if enriched_info:
            enriched_count += 1
            enriched_results.append(enriched_info)
        else:
            # 역지오코딩 실패 시 빈 값
            enriched_results.append({
                'ctp_cd': None,
                'ctp_nm': None,
                'sig_cd': None,
                'sig_nm': None,
                'emd_cd': None,
                'emd_nm': None
            })

        # API 호출 제한 (안정성)
        if enriched_info and ProcessConfig.API_REQUEST_DELAY > 0:
            time.sleep(ProcessConfig.API_REQUEST_DELAY)

    print(f"\n[OK] 완료!")
    print(f"좌표 기반 역지오코딩 성공: {enriched_count}개 ({enriched_count / total * 100:.1f}%)")
    print()

    return pd.DataFrame(enriched_results)


def process_school_data(school_type):
    """
    학교 데이터 처리 (Raw → 법정동코드 매칭 → Final)

    Args:
        school_type: 'high_school' 또는 'university'
    """
    start_time = time.time()

    # 파일명 설정
    if school_type == 'high_school':
        raw_pattern = "고등학교_raw_*.csv"
        fallback_raw_file = OutputConfig.RAW_HIGH_SCHOOLS
        display_name = "고등학교"
        is_university = False
    elif school_type == 'university':
        raw_pattern = "대학교_raw_*.csv"
        fallback_raw_file = OutputConfig.RAW_UNIVERSITIES
        display_name = "대학교"
        is_university = True
    else:
        raise ValueError(f"Invalid school_type: {school_type}")

    print(f"{display_name} 데이터 법정동코드 매칭 시작")
    print()

    try:
        # 1. Raw 데이터 로드 (최신 파일 자동 검색)
        raw_files = list(PathConfig.RAW_DATA_DIR.glob(raw_pattern))

        if not raw_files:
            # Fallback: 고정 파일명 시도
            input_path = PathConfig.RAW_DATA_DIR / fallback_raw_file
            if not input_path.exists():
                raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {PathConfig.RAW_DATA_DIR}/{raw_pattern}")
        else:
            # 날짜별로 정렬 (최신 파일 선택)
            raw_files.sort(key=lambda x: x.stem.split('_')[-1], reverse=True)
            input_path = raw_files[0]

        print(f"[INFO] 사용할 파일: {input_path.name}")

        df = pd.read_csv(input_path, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"{display_name} 데이터 로드: {len(df)}개")

        # 대학교인 경우 대학원 제외
        if is_university:
            original_count = len(df)
            df = df[~df['fac_nm'].str.contains('대학원', na=False)]
            excluded_count = original_count - len(df)
            if excluded_count > 0:
                print(f"[INFO] 대학원 제외: {excluded_count}개 (남은 데이터: {len(df)}개)")

        print()

        # 2. 고등학교와 대학교 모두 역지오코딩으로 법정동 정보 추출
        # 주소 기반 매칭 없이 오직 좌표(lat, lon)만 사용하여 정확한 법정동 정보 획득
        region_df = enrich_schools_with_reverse_geocoding(df, school_type=display_name)

        # 3. 주소 중복 제거
        region_df = apply_address_deduplication(region_df, step_label="[2/2] 주소 중복 제거")

        # 4. 최종 데이터 구성
        # 인덱스 리셋하여 df와 region_df의 행 순서 일치시키기
        df_reset = df.reset_index(drop=True)
        region_df_reset = region_df.reset_index(drop=True)

        # fac_cd 처리: 대학교는 int로 변환 후 문자열, 고등학교는 그대로 문자열
        if is_university:
            fac_cd_series = df_reset['fac_cd'].astype('Int64').astype(str)
        else:
            fac_cd_series = df_reset['fac_cd'].astype(str)

        final_df = pd.DataFrame({
            'fac_cd': fac_cd_series,
            'fac_nm': df_reset['fac_nm'],
            'ctp_cd': region_df_reset['ctp_cd'],
            'ctp_nm': region_df_reset['ctp_nm'],
            'sig_cd': region_df_reset['sig_cd'],
            'sig_nm': region_df_reset['sig_nm'],
            'emd_cd': region_df_reset['emd_cd'],
            'emd_nm': region_df_reset['emd_nm'],
            'all_addr_nm': df_reset['all_addr_nm'],
            'lat': pd.to_numeric(df_reset['lat'], errors='coerce'),
            'lon': pd.to_numeric(df_reset['lon'], errors='coerce')
        })

        # 5. 저장 (날짜별로 저장)
        from core.utils import ReferenceDataManager
        ref_manager = ReferenceDataManager(display_name)
        ref_manager.save_to_csv(final_df)

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print("=" * 70)
        print("작업 완료!")
        print("=" * 70)
        print(f"저장 파일: {ref_manager.get_latest_csv_file()}")
        print(f"총 {display_name} 수: {len(final_df)}개")
        print(f"컬럼 수: {len(final_df.columns)}개")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("=" * 70)

        # 샘플 데이터 출력 (인코딩 오류 방지)
        print(f"\n{display_name} 샘플 데이터 (첫 3개):")
        sample_cols = ['fac_nm', 'ctp_nm', 'sig_nm', 'emd_nm']
        try:
            print(final_df[sample_cols].head(3).to_string())
        except UnicodeEncodeError:
            # 인코딩 오류 시 안전하게 출력
            for idx, row in final_df[sample_cols].head(3).iterrows():
                print(f"{idx}: {row['fac_nm'][:20]}... | {row['ctp_nm']} {row['sig_nm']}")
        print()

    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        raise


def main():
    """
    메인 실행 함수 - 고등학교와 대학교 모두 처리
    """
    print("=" * 70)
    print("학교 데이터 법정동코드 매칭")
    print("=" * 70)
    print()

    # 고등학교 처리
    process_school_data('high_school')
    print("\n\n")

    # 대학교 처리
    process_school_data('university')


if __name__ == "__main__":
    main()
