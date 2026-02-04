"""
DART API를 통해 상장기업 데이터 수집 (날짜별 버전 관리)

API로 받은 데이터를 날짜별 CSV로 저장하고, 실패 시 가장 최근 CSV를 fallback으로 사용
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import requests
import pandas as pd
import zipfile
import io
import urllib3
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import APIConfig, PathConfig, OutputConfig, ProcessConfig
from core.utils import ReferenceDataManager

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_corp_codes():
    """
    DART에서 전체 기업 코드 목록 다운로드 (재시도 로직 포함)
    """
    print("=" * 70)
    print("[1/2] DART 기업 코드 목록 다운로드")
    print("=" * 70)

    url = f"{APIConfig.DART_BASE_URL}/corpCode.xml"
    params = {
        'crtfc_key': APIConfig.DART_API_KEY
    }

    print(f"요청 URL: {url}")

    # 재시도 로직
    for attempt in range(ProcessConfig.API_RETRY_COUNT):
        try:
            print(f"다운로드 시도 {attempt + 1}/{ProcessConfig.API_RETRY_COUNT}...")

            # HTTP 헤더 추가 (User-Agent)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=60,  # 타임아웃 증가 (기본 30초 -> 60초)
                verify=False
            )
            response.raise_for_status()

            # 응답 내용 확인 (디버깅)
            content_type = response.headers.get('Content-Type', '')
            content_length = len(response.content)
            print(f"[DEBUG] Content-Type: {content_type}, Size: {content_length} bytes")

            # ZIP인지 확인
            if not response.content.startswith(b'PK'):
                # ZIP 파일이 아닌 경우 처음 500자 출력
                preview = response.content[:500].decode('utf-8', errors='ignore')
                print(f"[ERROR] ZIP 파일이 아닙니다. 응답 내용:")
                print(preview)
                raise Exception(f"DART API가 ZIP 파일 대신 다른 형식을 반환했습니다 (Content-Type: {content_type})")

            # ZIP 파일 압축 해제
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_content = zip_file.read('CORPCODE.xml')

            print("[OK] 다운로드 완료")
            return xml_content

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"[WARNING] 연결 실패 (시도 {attempt + 1}/{ProcessConfig.API_RETRY_COUNT}): {e}")
            if attempt < ProcessConfig.API_RETRY_COUNT - 1:
                wait_time = 3 * (attempt + 1)  # 3초, 6초, 9초 대기
                print(f"  -> {wait_time}초 후 재시도...")
                time.sleep(wait_time)
            else:
                print(f"[ERROR] 최대 재시도 횟수 초과")
                raise
        except Exception as e:
            print(f"[ERROR] 오류 발생: {e}")
            raise

def parse_corp_codes(xml_content):
    """
    XML에서 기업 코드 파싱 (상장 + 비상장 모두 포함)
    """
    print("\n기업 코드 XML 파싱 중...")

    root = ET.fromstring(xml_content)
    companies = []

    for corp in root.findall('list'):
        corp_code = corp.find('corp_code').text
        corp_name = corp.find('corp_name').text
        stock_code = corp.find('stock_code').text if corp.find('stock_code') is not None else None
        modify_date = corp.find('modify_date').text if corp.find('modify_date') is not None else None

        # 상장 기업만 포함 (stock_code가 있는 경우만)
        if stock_code and stock_code.strip():
            companies.append({
                'corp_code': corp_code,
                'corp_name': corp_name,
                'stock_code': stock_code.strip(),
                'modify_date': modify_date
            })

    df = pd.DataFrame(companies)
    print(f"[OK] 상장 기업 {len(df)}개 발견")

    return df

def get_company_info(corp_code, retry_count=None):
    """
    특정 기업의 상세 정보 조회 (재시도 로직 포함)

    Args:
        corp_code: 기업 코드
        retry_count: 재시도 횟수 (None이면 설정값 사용)

    Returns:
        기업 정보 딕셔너리 또는 None
    """
    if retry_count is None:
        retry_count = ProcessConfig.API_RETRY_COUNT

    url = f"{APIConfig.DART_BASE_URL}/company.json"
    params = {
        'crtfc_key': APIConfig.DART_API_KEY,
        'corp_code': corp_code
    }

    for attempt in range(retry_count):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=ProcessConfig.API_TIMEOUT,
                verify=False
            )
            response.raise_for_status()
            data = response.json()

            if data.get('status') == '000':
                return data
            else:
                return None

        except requests.exceptions.Timeout:
            if attempt < retry_count - 1:
                time.sleep(0.5)
                continue
            return None
        except requests.exceptions.RequestException:
            if attempt < retry_count - 1:
                time.sleep(0.5)
                continue
            return None
        except Exception:
            return None

    return None

def process_single_company(idx, row):
    """
    단일 기업 정보 수집 (병렬 처리용)

    Args:
        idx: 데이터프레임의 인덱스
        row: 기업 기본 정보

    Returns:
        (idx, company_info_dict, error_info) 튜플
    """
    corp_code = row['corp_code']
    corp_name = row['corp_name']
    info = get_company_info(corp_code)

    # Rate limiting (DART 전용 설정)
    if ProcessConfig.DART_API_REQUEST_DELAY > 0:
        time.sleep(ProcessConfig.DART_API_REQUEST_DELAY)

    error_info = None

    if info and info.get('status') == '000':
        result = {
            'corp_code': corp_code,
            'corp_name': info.get('corp_name', ''),
            'corp_name_eng': info.get('corp_name_eng', ''),
            'stock_code': info.get('stock_code', ''),
            'ceo_nm': info.get('ceo_nm', ''),
            'corp_cls': info.get('corp_cls', ''),
            'jurir_no': info.get('jurir_no', ''),
            'bizr_no': info.get('bizr_no', ''),
            'adres': info.get('adres', ''),
            'hm_url': info.get('hm_url', ''),
            'ir_url': info.get('ir_url', ''),
            'phn_no': info.get('phn_no', ''),
            'fax_no': info.get('fax_no', ''),
            'induty_code': info.get('induty_code', ''),
            'est_dt': info.get('est_dt', ''),
            'acc_mt': info.get('acc_mt', ''),
        }
    else:
        # 실패 정보 기록
        if info:
            status = info.get('status', 'UNKNOWN')
            message = info.get('message', 'No message')
            error_info = {
                'corp_code': corp_code,
                'corp_name': corp_name,
                'status': status,
                'message': message
            }
        else:
            error_info = {
                'corp_code': corp_code,
                'corp_name': corp_name,
                'status': 'NO_RESPONSE',
                'message': 'API returned None (timeout or network error)'
            }

        result = {
            'corp_code': corp_code,
            'corp_name': row['corp_name'],
            'stock_code': row['stock_code'],
            'corp_name_eng': '',
            'ceo_nm': '',
            'corp_cls': '',
            'jurir_no': '',
            'bizr_no': '',
            'adres': '',
            'hm_url': '',
            'ir_url': '',
            'phn_no': '',
            'fax_no': '',
            'induty_code': '',
            'est_dt': '',
            'acc_mt': '',
        }

    return idx, result, error_info

def collect_all_company_info(corp_codes_df):
    """
    전체 기업의 상세 정보 수집 (안정화된 병렬 처리)
    """
    print("=" * 70)
    print("[2/2] 기업 상세 정보 수집 (안정화된 병렬 처리)")
    print("=" * 70)
    print(f"총 {len(corp_codes_df)}개 기업 정보 수집 시작...")
    print(f"병렬 처리: {ProcessConfig.DART_MAX_WORKERS}개 스레드 동시 실행")
    print(f"재시도: {ProcessConfig.API_RETRY_COUNT}회, 딜레이: {ProcessConfig.DART_API_REQUEST_DELAY}초")
    print()

    # 결과를 인덱스별로 저장 (순서 보장)
    results = {}
    errors = []  # 실패 정보 수집
    total = len(corp_codes_df)
    completed = 0
    success_count = 0
    failed_count = 0

    # 병렬 처리 (DART 전용 설정)
    with ThreadPoolExecutor(max_workers=ProcessConfig.DART_MAX_WORKERS) as executor:
        # 모든 작업 제출 (인덱스와 함께)
        futures = {
            executor.submit(process_single_company, idx, row): idx
            for idx, row in corp_codes_df.iterrows()
        }

        # 완료된 작업 처리
        for future in as_completed(futures):
            completed += 1
            # 진행 상황 출력 빈도 조정 (100 -> 500)
            if completed % 500 == 0 or completed == 1 or completed == total:
                print(f"진행 중: {completed}/{total} ({completed / total * 100:.1f}%) - 성공: {success_count}, 실패: {failed_count}")

            try:
                idx, result, error_info = future.result(timeout=ProcessConfig.API_TIMEOUT + 10)
                results[idx] = result

                # 성공/실패 판단 (주소가 있으면 성공)
                if result.get('adres') and result.get('adres').strip():
                    success_count += 1
                else:
                    failed_count += 1
                    if error_info:
                        errors.append(error_info)

            except Exception as e:
                # 실패한 작업의 인덱스 찾기
                idx = futures[future]
                row = corp_codes_df.loc[idx]
                results[idx] = {
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'stock_code': row['stock_code'],
                    'corp_name_eng': '',
                    'ceo_nm': '',
                    'corp_cls': '',
                    'jurir_no': '',
                    'bizr_no': '',
                    'adres': '',
                    'hm_url': '',
                    'ir_url': '',
                    'phn_no': '',
                    'fax_no': '',
                    'induty_code': '',
                    'est_dt': '',
                    'acc_mt': '',
                }
                failed_count += 1
                errors.append({
                    'corp_code': row['corp_code'],
                    'corp_name': row['corp_name'],
                    'status': 'EXCEPTION',
                    'message': str(e)
                })

    # 인덱스 순서대로 결과 정렬
    sorted_results = [results[idx] for idx in sorted(results.keys())]
    print(f"\n[OK] 수집 완료: {len(sorted_results)}개 기업")
    print(f"  - API 성공 (주소 있음): {success_count}개 ({success_count / total * 100:.1f}%)")
    print(f"  - API 실패 (주소 없음): {failed_count}개 ({failed_count / total * 100:.1f}%)")
    print()

    # 실패 원인 분석
    if errors:
        print("=" * 70)
        print(f"실패 원인 분석 (총 {len(errors)}건)")
        print("=" * 70)

        # 상태 코드별로 그룹화
        status_counts = {}
        for error in errors:
            status = error['status']
            status_counts[status] = status_counts.get(status, 0) + 1

        print("\n상태 코드별 실패 건수:")
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {status}: {count}건")

        # 실패 샘플 출력 (최대 10개)
        print(f"\n실패 샘플 (최대 10개):")
        for i, error in enumerate(errors[:10]):
            print(f"\n  [{i+1}] {error['corp_name']} ({error['corp_code']})")
            print(f"      상태: {error['status']}")
            print(f"      메시지: {error['message']}")

        if len(errors) > 10:
            print(f"\n  ... 외 {len(errors) - 10}건 더 있음")
        print()

    return pd.DataFrame(sorted_results)

def cleanup_old_raw_files(data_name: str, keep_file: Path):
    """
    이전 날짜의 raw 파일들 삭제 (최신 파일만 유지)

    Args:
        data_name: 데이터 이름 (예: "기업위치")
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
    메인 실행 함수 (Fallback 지원)

    Returns:
        bool: True=정상 수집, False=Fallback 사용
    """
    start_time = time.time()

    # 날짜 포함 파일명 생성
    today = datetime.now().strftime('%Y%m%d')
    raw_filename = f"기업위치_raw_{today}.csv"
    output_path = PathConfig.RAW_DATA_DIR / raw_filename

    used_fallback = False

    # ReferenceDataManager 초기화
    ref_manager = ReferenceDataManager("기업위치")

    try:
        # 1. 기업 코드 목록 다운로드
        xml_content = download_corp_codes()

        # 2. XML 파싱
        corp_codes_df = parse_corp_codes(xml_content)

        # 3. 기업 상세 정보 수집
        companies_df = collect_all_company_info(corp_codes_df)

        # 4. data/raw 폴더에 저장
        PathConfig.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        companies_df.to_csv(output_path, index=False, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"[OK] Raw 데이터 저장: {output_path.name}")

        # 5. 이전 날짜 파일들 삭제
        cleanup_old_raw_files("기업위치", output_path)

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print("\n" + "=" * 70)
        print("작업 완료!")
        print("=" * 70)
        print(f"Raw 데이터: {output_path}")
        print(f"총 기업 수: {len(companies_df)}개")
        print(f"컬럼 수: {len(companies_df.columns)}개")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("=" * 70)

        # 샘플 데이터 출력
        print("\n샘플 데이터 (첫 3개):")
        print(companies_df.head(3).to_string())
        print()

        # 주소 정보 통계
        print(f"주소 정보 존재: {companies_df['adres'].notna().sum()}개 ({companies_df['adres'].notna().sum() / len(companies_df) * 100:.1f}%)")
        print(f"업종 코드 존재: {companies_df['induty_code'].notna().sum()}개 ({companies_df['induty_code'].notna().sum() / len(companies_df) * 100:.1f}%)")

        return True  # 정상 수집

    except Exception as e:
        print(f"\n[ERROR] DART API 호출 실패: {e}")

        # Fallback: raw 폴더의 기존 데이터 사용
        if ProcessConfig.USE_FALLBACK:
            print("[INFO] Fallback: API 호출 실패, raw/ 폴더의 기존 데이터를 사용합니다.")

            # 최신 Raw 파일 찾기
            pattern = "기업위치_raw_*.csv"
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
