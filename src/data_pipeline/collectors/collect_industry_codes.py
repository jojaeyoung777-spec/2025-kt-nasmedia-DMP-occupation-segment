"""
한국무역보험공사 업종코드 API에서 데이터 수집 (날짜별 버전 관리)

API로 받은 데이터를 날짜별 CSV로 저장하고, 실패 시 가장 최근 CSV를 fallback으로 사용
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import requests
import time
import json
import pandas as pd
import urllib3
from datetime import datetime
from typing import List
from config.settings import PathConfig, ProcessConfig, APIConfig

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def fetch_all_pages():
    """
    API에서 전체 데이터 페이징 처리

    Swagger API 스펙:
    - 엔드포인트: https://api.odcloud.kr/api/15064297/v1/uddi:07a7ea27-b8b2-4698-835c-c3b950cefb34
    - 인증: serviceKey 쿼리 파라미터
    - 파라미터: page, perPage, returnType
    - 응답: {"page": 1, "perPage": 10, "totalCount": ..., "data": [...]}
    """
    all_data = []
    page = 1
    per_page = 1000
    service_key = APIConfig.LEGAL_DONG_API_KEY  # 공공데이터포털 API 키 공유

    while True:
        params = {
            'serviceKey': service_key,
            'page': page,
            'perPage': per_page,
            'returnType': 'JSON'
        }

        try:
            response = requests.get(
                APIConfig.INDUSTRY_API_URL,
                params=params,
                timeout=ProcessConfig.API_TIMEOUT,
                verify=False
            )
            response.raise_for_status()
            data = response.json()

            # 디버깅: 첫 페이지 응답 구조 출력
            if page == 1:
                print(f"[DEBUG] API 응답 타입: {type(data)}")
                if isinstance(data, dict):
                    print(f"[DEBUG] API 응답 키: {list(data.keys())}")
                    print(f"[DEBUG] 총 데이터 수: {data.get('totalCount', 0)}")
                    print(f"[DEBUG] 현재 페이지 데이터 수: {data.get('currentCount', 0)}")

                    # 첫 번째 데이터 항목의 모든 키 출력
                    items = data.get('data', [])
                    if items:
                        print(f"[DEBUG] 첫 번째 항목의 모든 키: {list(items[0].keys())}")
                        print(f"[DEBUG] 첫 번째 항목 전체 데이터:")
                        for key, value in items[0].items():
                            print(f"  - {key}: {value}")

            # 응답 구조 검증
            if not isinstance(data, dict):
                raise Exception(f"예상하지 못한 응답 타입: {type(data)}")

            # 에러 응답 체크 (401, 500 등)
            if 'data' not in data:
                error_msg = data.get('message', data.get('error', str(data)))
                raise Exception(f"API 응답에 'data' 키가 없습니다. 에러: {error_msg}")

            items = data.get('data', [])
            if not items:
                if page == 1:
                    print(f"[INFO] 조회된 데이터가 없습니다")
                break

            all_data.extend(items)
            print(f"  페이지 {page}: {len(items)}개 수집 - 누적: {len(all_data)}개")

            # 전체 개수 확인
            total_count = data.get('totalCount', 0)
            if len(all_data) >= total_count or len(items) < per_page:
                print(f"[OK] 전체 데이터 수집 완료: {len(all_data)}개 (총 {total_count}개)")
                break

            page += 1

            # API 호출 제한 (과도한 요청 방지)
            if page > 100:  # 최대 100페이지
                print(f"[WARNING] 최대 페이지 수 도달. 수집 중단.")
                break

            # Rate limiting
            time.sleep(ProcessConfig.API_REQUEST_DELAY)

        except requests.exceptions.RequestException as e:
            raise Exception(f"API 호출 실패: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"JSON 파싱 실패: {str(e)}. 응답: {response.text[:200]}")

    return all_data


def build_hierarchy(df: pd.DataFrame) -> pd.DataFrame:
    """
    업종코드를 5단계 계층 구조로 변환

    KSIC (한국표준산업분류) 구조:
    - Depth 1: 알파벳 대분류 (A~U) - SRR업종코드 필드
    - Depth 2: 2자리 중분류 (예: 42)
    - Depth 3: 3자리 소분류 (예: 422)
    - Depth 4: 4자리 세분류 (예: 4220)
    - Depth 5: 5자리 세세분류 (예: 42209)

    Args:
        df: 원본 업종코드 DataFrame

    Returns:
        계층 구조가 추가된 DataFrame
    """
    print("\n[INFO] 업종코드를 5단계 계층 구조로 변환 중...")

    # 업종코드를 문자열로 변환
    df['업종코드'] = df['업종코드'].astype(str).str.strip()
    df['원본업종코드'] = df['원본업종코드'].astype(str).str.strip()

    # 대분류 매핑 (알파벳 -> 한글명) - KSIC 표준산업분류
    depth1_map = {
        'A': '농업, 임업 및 어업',
        'B': '광업',
        'C': '제조업',
        'D': '전기, 가스, 증기 및 공기조절 공급업',
        'E': '수도, 하수 및 폐기물 처리, 원료 재생업',
        'F': '건설업',
        'G': '도매 및 소매업',
        'H': '운수 및 창고업',
        'I': '숙박 및 음식점업',
        'J': '정보통신업',
        'K': '금융 및 보험업',
        'L': '부동산업',
        'M': '전문, 과학 및 기술 서비스업',
        'N': '사업시설 관리, 사업 지원 및 임대 서비스업',
        'O': '공공행정, 국방 및 사회보장 행정',
        'P': '교육 서비스업',
        'Q': '보건업 및 사회복지 서비스업',
        'R': '예술, 스포츠 및 여가관련 서비스업',
        'S': '협회 및 단체, 수리 및 기타 개인 서비스업',
        'T': '가구 내 고용활동 및 달리 분류되지 않은 자가소비 생산활동',
        'U': '국제 및 외국기관'
    }

    # Depth 2~5별로 딕셔너리 생성 (코드 길이로 판단)
    depth_dicts = {2: {}, 3: {}, 4: {}, 5: {}}

    for idx, row in df.iterrows():
        code = row['업종코드']
        name = row['업종한글명']

        if len(code) == 2:
            depth_dicts[2][code] = name
        elif len(code) == 3:
            depth_dicts[3][code] = name
        elif len(code) == 4:
            depth_dicts[4][code] = name
        elif len(code) == 5:
            depth_dicts[5][code] = name

    print(f"  - Depth 1 (대분류): {len(depth1_map)}개")
    print(f"  - Depth 2 (중분류, 2자리): {len(depth_dicts[2])}개")
    print(f"  - Depth 3 (소분류, 3자리): {len(depth_dicts[3])}개")
    print(f"  - Depth 4 (세분류, 4자리): {len(depth_dicts[4])}개")
    print(f"  - Depth 5 (세세분류, 5자리): {len(depth_dicts[5])}개")

    # 계층 정보를 저장할 컬럼 초기화
    df['업종코드_depth1'] = ''
    df['업종명_depth1'] = ''
    df['업종코드_depth2'] = ''
    df['업종명_depth2'] = ''
    df['업종코드_depth3'] = ''
    df['업종명_depth3'] = ''
    df['업종코드_depth4'] = ''
    df['업종명_depth4'] = ''
    df['업종코드_depth5'] = ''
    df['업종명_depth5'] = ''

    # 각 행에 대해 계층 구조 생성
    for idx, row in df.iterrows():
        code = row['업종코드']
        original_code = row['원본업종코드']

        # Depth 1: 원본업종코드의 첫 글자(알파벳)를 대분류로 사용
        if len(original_code) > 0:
            first_char = original_code[0].upper()
            if first_char in depth1_map:
                df.at[idx, '업종코드_depth1'] = first_char
                df.at[idx, '업종명_depth1'] = depth1_map[first_char]

        # Depth 2~5: 코드를 앞에서부터 잘라서 상위 계층 찾기
        if len(code) >= 2:
            code_d2 = code[:2]
            if code_d2 in depth_dicts[2]:
                df.at[idx, '업종코드_depth2'] = code_d2
                df.at[idx, '업종명_depth2'] = depth_dicts[2][code_d2]

        if len(code) >= 3:
            code_d3 = code[:3]
            if code_d3 in depth_dicts[3]:
                df.at[idx, '업종코드_depth3'] = code_d3
                df.at[idx, '업종명_depth3'] = depth_dicts[3][code_d3]

        if len(code) >= 4:
            code_d4 = code[:4]
            if code_d4 in depth_dicts[4]:
                df.at[idx, '업종코드_depth4'] = code_d4
                df.at[idx, '업종명_depth4'] = depth_dicts[4][code_d4]

        if len(code) == 5:
            if code in depth_dicts[5]:
                df.at[idx, '업종코드_depth5'] = code
                df.at[idx, '업종명_depth5'] = depth_dicts[5][code]

    # 최종 컬럼 선택 (계층 구조만)
    result_df = df[[
        '업종코드_depth1', '업종명_depth1',
        '업종코드_depth2', '업종명_depth2',
        '업종코드_depth3', '업종명_depth3',
        '업종코드_depth4', '업종명_depth4',
        '업종코드_depth5', '업종명_depth5'
    ]].copy()

    # 중복 제거 (같은 계층 구조는 1개만)
    result_df = result_df.drop_duplicates()

    print(f"[OK] 계층 구조 변환 완료: {len(result_df)}개 (중복 제거 전: {len(df)}개)")

    return result_df


def save_raw_to_csv(data_list: List[dict]):
    """
    API 원본 데이터를 raw 폴더에 저장

    Args:
        data_list: API에서 받은 전체 데이터 리스트

    Returns:
        저장된 raw CSV 파일 경로
    """
    today = datetime.now().strftime('%Y%m%d')
    raw_filename = f"업종코드_raw_{today}.csv"
    raw_path = PathConfig.RAW_DATA_DIR / raw_filename

    # DataFrame 생성 및 저장
    df = pd.DataFrame(data_list)
    PathConfig.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(raw_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)

    print(f"[OK] Raw 데이터 저장: {raw_filename} ({len(df)}개)")

    # 이전 날짜의 raw 파일들 삭제
    cleanup_old_files("업종코드_raw_*.csv", raw_path, PathConfig.RAW_DATA_DIR)

    return raw_path


def process_and_save_final(raw_csv_path: Path):
    """
    Raw 데이터를 읽어서 계층 구조로 변환 후 final 폴더에 저장

    Args:
        raw_csv_path: Raw CSV 파일 경로

    Returns:
        저장된 final CSV 파일 경로
    """
    print(f"\n[INFO] Raw 데이터 전처리 시작: {raw_csv_path.name}")

    # Raw 데이터 읽기
    df = pd.read_csv(raw_csv_path, encoding=ProcessConfig.ENCODING_DEFAULT)
    print(f"[DEBUG] Raw 데이터 컬럼: {list(df.columns)}")

    # 계층 구조로 변환
    hierarchy_df = build_hierarchy(df)

    # Final 폴더에 저장
    today = datetime.now().strftime('%Y%m%d')
    final_filename = f"업종코드_{today}.csv"
    final_path = PathConfig.FINAL_DATA_DIR / final_filename

    PathConfig.FINAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    hierarchy_df.to_csv(final_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)
    print(f"[OK] Final 데이터 저장: {final_filename} ({len(hierarchy_df)}개)")

    # 이전 날짜의 final 파일들 삭제
    cleanup_old_files("업종코드_*.csv", final_path, PathConfig.FINAL_DATA_DIR)

    return final_path


def cleanup_old_files(pattern: str, keep_file: Path, target_dir: Path = None):
    """
    이전 날짜의 파일들 삭제 (최신 파일만 유지)

    Args:
        pattern: 파일 패턴 (예: "업종코드_*.csv")
        keep_file: 유지할 파일 경로
        target_dir: 삭제할 디렉토리 (기본값: FINAL_DATA_DIR)
    """
    if target_dir is None:
        target_dir = PathConfig.FINAL_DATA_DIR

    if not target_dir.exists():
        return

    old_files = list(target_dir.glob(pattern))
    deleted_count = 0

    for file_path in old_files:
        if file_path != keep_file:
            try:
                file_path.unlink()
                deleted_count += 1
            except Exception:
                pass

    if deleted_count > 0:
        print(f"[OK] 이전 파일 {deleted_count}개 삭제됨")


def main():
    """
    메인 실행 함수 (Fallback 지원)

    Returns:
        bool: True=정상 수집, False=Fallback 사용
    """
    start_time = time.time()

    print("=" * 70)
    print("업종코드 API 데이터 수집")
    print("=" * 70)
    print()

    try:
        # 1. API에서 데이터 가져오기
        print("[INFO] API에서 업종코드 다운로드 시도...")
        all_data = fetch_all_pages()

        if not all_data:
            raise Exception("API에서 데이터를 가져오지 못했습니다")

        # 2. Raw 데이터 저장
        raw_path = save_raw_to_csv(all_data)

        # 3. 전처리 후 Final 저장
        final_path = process_and_save_final(raw_path)

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print()
        print("=" * 70)
        print("작업 완료!")
        print("=" * 70)
        print(f"Raw 파일: {raw_path}")
        print(f"Final 파일: {final_path}")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("=" * 70)
        print()

        # 샘플 데이터 출력
        df = pd.read_csv(final_path, encoding=ProcessConfig.ENCODING_DEFAULT)
        print("샘플 데이터 (첫 3개):")
        print(df.head(3).to_string())
        print()

        return True  # 정상 수집

    except Exception as e:
        print(f"\n[ERROR] 업종코드 API 호출 실패: {e}")

        # Fallback: raw 폴더의 기존 데이터를 읽어서 전처리
        if ProcessConfig.USE_FALLBACK:
            print("[INFO] Fallback: API 호출 실패, raw/ 폴더의 기존 데이터로 전처리를 수행합니다.")

            # 최신 Raw CSV 파일 찾기
            raw_files = list(PathConfig.RAW_DATA_DIR.glob("업종코드_raw_*.csv"))
            if raw_files:
                # 날짜별로 정렬
                raw_files.sort(key=lambda x: x.stem.split('_')[-1] if '_' in x.stem else '', reverse=True)
                latest_raw = raw_files[0]
                print(f"[INFO] 기존 Raw 파일 사용: {latest_raw.name}")

                # Raw 데이터를 전처리하여 Final 저장
                final_path = process_and_save_final(latest_raw)

                print(f"[OK] Fallback 완료: {final_path.name}")
                return False  # Fallback 사용
            else:
                print("[ERROR] Fallback용 Raw 파일도 찾을 수 없습니다.")
                return False
        else:
            print("[ERROR] Fallback이 비활성화되어 있습니다.")
            return False


if __name__ == "__main__":
    main()
