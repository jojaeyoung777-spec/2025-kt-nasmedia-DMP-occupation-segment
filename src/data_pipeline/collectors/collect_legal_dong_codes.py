"""
공공데이터포털 법정동코드 API에서 데이터 수집 (날짜별 버전 관리)

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

    공식 API 문서:
    - 요청: ServiceKey(URL 인코딩), type(json/xml), pageNo, numOfRows, flag(Y)
    - 응답: {"StanReginCd": {"head": {...}, "row": [...]}}
    - 성공: resultCode == "INFO-0"
    """
    all_data = []
    page_no = 1
    num_of_rows = 1000
    service_key = APIConfig.LEGAL_DONG_API_KEY

    while True:
        params = {
            'ServiceKey': service_key,
            'type': 'json',
            'pageNo': page_no,
            'numOfRows': num_of_rows,
            'flag': 'Y'
        }

        try:
            response = requests.get(
                APIConfig.LEGAL_DONG_API_URL,
                params=params,
                timeout=ProcessConfig.API_TIMEOUT,
                verify=False
            )
            response.raise_for_status()

            data = response.json()

            # 디버깅: 첫 페이지 응답 구조 출력
            if page_no == 1:
                print(f"[DEBUG] API 응답 타입: {type(data)}")
                if isinstance(data, dict):
                    print(f"[DEBUG] API 응답 최상위 키: {list(data.keys())}")
                print(f"[DEBUG] 원본 응답 샘플: {str(data)[:500]}...")

            # 표준 응답 구조 파싱
            if not isinstance(data, dict):
                raise Exception(f"예상하지 못한 응답 타입: {type(data)}. 응답: {str(data)[:200]}")

            # StanReginCd 키 확인
            if 'StanReginCd' not in data:
                if 'cmmMsgHeader' in data:
                    error_msg = data.get('cmmMsgHeader', {})
                    raise Exception(f"API 에러: {error_msg}")
                else:
                    raise Exception(f"'StanReginCd' 키를 찾을 수 없음. 응답 키: {list(data.keys())}")

            stan_regin_cd = data['StanReginCd']

            # 실제 API 응답: StanReginCd는 리스트 형태
            if not isinstance(stan_regin_cd, list) or len(stan_regin_cd) < 2:
                raise Exception(f"예상치 못한 StanReginCd 구조: {type(stan_regin_cd)}")

            # head 정보 추출
            head_container = stan_regin_cd[0]
            if 'head' not in head_container:
                raise Exception("응답에 'head' 정보가 없습니다")

            head_list = head_container['head']
            if not isinstance(head_list, list):
                raise Exception(f"head가 리스트가 아닙니다: {type(head_list)}")

            total_count = 0
            result_code = ''
            result_msg = ''

            for item in head_list:
                if 'totalCount' in item:
                    total_count = int(item['totalCount'])
                elif 'RESULT' in item:
                    result = item['RESULT']
                    result_code = result.get('resultCode', '')
                    result_msg = result.get('resultMsg', '')

            if page_no == 1:
                print(f"[DEBUG] API 결과: [{result_code}] {result_msg}, 총 {total_count}개")

            # 에러 코드 체크
            if result_code not in ['INFO-0', 'INFO-200', '0']:
                raise Exception(f"API 오류 코드: [{result_code}] {result_msg}")

            # row 데이터 추출
            row_container = stan_regin_cd[1]
            if 'row' not in row_container:
                raise Exception("응답에 'row' 정보가 없습니다")

            rows = row_container['row']

            # 데이터가 없으면 종료
            if not rows or result_code == 'INFO-200':
                if page_no == 1:
                    print(f"[INFO] 조회된 데이터가 없습니다")
                break

            # row가 단일 dict인 경우 리스트로 변환
            if isinstance(rows, dict):
                rows = [rows]

            all_data.extend(rows)
            print(f"  페이지 {page_no}: {len(rows)}개 수집 - 누적: {len(all_data)}개")

            # 전체 개수 확인
            if len(all_data) >= total_count or len(rows) < num_of_rows:
                print(f"[OK] 전체 데이터 수집 완료: {len(all_data)}개 (총 {total_count}개)")
                break

            page_no += 1

            # API 호출 제한
            if page_no > 100:
                print(f"[WARNING] 최대 페이지 수 도달. 수집 중단.")
                break

            # Rate limiting
            time.sleep(ProcessConfig.API_REQUEST_DELAY)

        except requests.exceptions.RequestException as e:
            raise Exception(f"API 호출 실패: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"JSON 파싱 실패: {str(e)}. 응답: {response.text[:200]}")

    return all_data


def save_raw_to_csv(data_list: List[dict]):
    """
    API 원본 데이터를 raw 폴더에 저장

    Args:
        data_list: API에서 받은 전체 데이터 리스트

    Returns:
        저장된 raw CSV 파일 경로
    """
    today = datetime.now().strftime('%Y%m%d')
    raw_filename = f"법정동코드_raw_{today}.csv"
    raw_path = PathConfig.RAW_DATA_DIR / raw_filename

    # DataFrame 생성 및 저장
    df = pd.DataFrame(data_list)
    PathConfig.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(raw_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)

    print(f"[OK] Raw 데이터 저장: {raw_filename} ({len(df)}개)")

    # 이전 날짜의 raw 파일들 삭제
    cleanup_old_files("법정동코드_raw_*.csv", raw_path, PathConfig.RAW_DATA_DIR)

    return raw_path


def process_and_save_final(raw_csv_path: Path):
    """
    Raw 데이터를 읽어서 전처리 후 final 폴더에 저장

    Args:
        raw_csv_path: Raw CSV 파일 경로

    Returns:
        저장된 final CSV 파일 경로
    """
    print(f"\n[INFO] Raw 데이터 전처리 시작: {raw_csv_path.name}")

    # Raw 데이터 읽기
    df = pd.read_csv(raw_csv_path, encoding=ProcessConfig.ENCODING_DEFAULT)

    # 필요한 컬럼만 선택
    if 'region_cd' in df.columns and 'locatadd_nm' in df.columns:
        df = df[['region_cd', 'locatadd_nm']]
        df.columns = ['법정동코드', '법정동명']

    # Final 폴더에 저장
    today = datetime.now().strftime('%Y%m%d')
    final_filename = f"법정동코드_{today}.csv"
    final_path = PathConfig.FINAL_DATA_DIR / final_filename

    PathConfig.FINAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(final_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)
    print(f"[OK] Final 데이터 저장: {final_filename} ({len(df)}개)")

    # 이전 날짜의 final 파일들 삭제
    cleanup_old_files("법정동코드_*.csv", final_path, PathConfig.FINAL_DATA_DIR)

    return final_path


def cleanup_old_files(pattern: str, keep_file: Path, target_dir: Path = None):
    """
    이전 날짜의 파일들 삭제 (최신 파일만 유지)

    Args:
        pattern: 파일 패턴 (예: "법정동코드_*.csv")
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
    print("법정동코드 API 데이터 수집")
    print("=" * 70)
    print()

    try:
        # 1. API에서 데이터 가져오기
        print("[INFO] API에서 법정동코드 다운로드 시도...")
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
        print(f"\n[ERROR] 법정동코드 API 호출 실패: {e}")

        # Fallback: raw 폴더의 기존 데이터를 읽어서 전처리
        if ProcessConfig.USE_FALLBACK:
            print("[INFO] Fallback: API 호출 실패, raw/ 폴더의 기존 데이터로 전처리를 수행합니다.")

            # 최신 Raw CSV 파일 찾기
            raw_files = list(PathConfig.RAW_DATA_DIR.glob("법정동코드_raw_*.csv"))
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
