"""
공통 유틸리티 함수

모든 데이터 수집 및 전처리 모듈에서 사용하는 공통 기능
"""
import pandas as pd
import re
from pathlib import Path
from typing import Callable, Optional
from datetime import datetime
from config.settings import ProcessConfig, PathConfig


def run_with_fallback(
    api_function: Callable,
    fallback_file: Path,
    encoding: str = "utf-8-sig",
    task_name: str = "작업"
) -> pd.DataFrame:
    """
    API 함수 실행 후 실패 시 fallback 파일 사용

    Args:
        api_function: 실행할 API 함수
        fallback_file: Fallback으로 사용할 CSV 파일 경로
        encoding: CSV 파일 인코딩
        task_name: 작업 이름 (로깅용)

    Returns:
        DataFrame: API 결과 또는 Fallback 파일 데이터

    Raises:
        Exception: API 실패 및 Fallback 파일 없음
    """
    try:
        # API 함수 실행
        result = api_function()
        return result

    except Exception as e:
        print(f"\n[ERROR] {task_name} 실패: {e}")

        # Fallback: 기존 파일 사용
        if ProcessConfig.USE_FALLBACK and fallback_file.exists():
            print(f"[INFO] Fallback: 기존 파일에서 데이터 로드")
            df = pd.read_csv(fallback_file, encoding=encoding)
            print(f"[OK] 기존 파일에서 {len(df)}개 데이터 로드")
            return df
        else:
            if not ProcessConfig.USE_FALLBACK:
                print(f"[ERROR] Fallback이 비활성화되어 있습니다 (USE_FALLBACK=False)")
            else:
                print(f"[ERROR] Fallback 파일이 없습니다: {fallback_file}")
            raise


def save_dataframe_safely(
    df: pd.DataFrame,
    output_path: Path,
    encoding: str = "utf-8-sig",
    task_name: str = "데이터"
) -> None:
    """
    DataFrame을 안전하게 CSV로 저장

    Args:
        df: 저장할 DataFrame
        output_path: 저장 경로
        encoding: CSV 파일 인코딩
        task_name: 작업 이름 (로깅용)
    """
    try:
        # 디렉토리가 없으면 생성
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # CSV 저장
        df.to_csv(output_path, index=False, encoding=encoding)
        print(f"[OK] {task_name} 저장 완료: {output_path.name}")

    except Exception as e:
        print(f"[ERROR] {task_name} 저장 실패: {e}")
        raise


def check_file_exists_skip(file_path: Path, task_name: str = "데이터") -> bool:
    """
    파일이 이미 존재하는지 확인하고 존재하면 Skip 메시지 출력

    Args:
        file_path: 확인할 파일 경로
        task_name: 작업 이름 (로깅용)

    Returns:
        bool: 파일이 존재하면 True, 없으면 False
    """
    if file_path.exists():
        print(f"[SKIP] {task_name} 파일이 이미 존재합니다: {file_path.name}")
        print(f"       기존 데이터를 사용합니다.")
        return True
    return False


class ReferenceDataManager:
    """
    최종 데이터 관리 클래스 (Final Data Manager)

    날짜별 CSV 파일 저장, 이전 파일 삭제, 최신 파일 로드 등을 처리
    전처리까지 완료된 최종 파일을 data/final/에 저장하고 관리합니다.
    """

    def __init__(self, data_name: str, final_dir: Path = None):
        """
        Args:
            data_name: 데이터 이름 (예: "고등학교위치", "대학교위치", "기업위치")
            final_dir: final 디렉토리 경로 (None이면 config에서 사용)
        """
        self.data_name = data_name
        self.final_dir = final_dir or PathConfig.FINAL_DATA_DIR

    def save_to_csv(self, df: pd.DataFrame, encoding: str = None) -> Path:
        """
        DataFrame을 날짜별 CSV로 저장하고 이전 파일 삭제

        Args:
            df: 저장할 DataFrame
            encoding: CSV 인코딩 (None이면 설정값 사용)

        Returns:
            저장된 파일 경로
        """
        if encoding is None:
            encoding = ProcessConfig.ENCODING_DEFAULT

        # 오늘 날짜로 파일명 생성
        today = datetime.now().strftime('%Y%m%d')
        csv_filename = f"{self.data_name}_{today}.csv"
        csv_path = self.final_dir / csv_filename

        # 디렉토리 생성
        self.final_dir.mkdir(parents=True, exist_ok=True)

        # CSV 저장
        df.to_csv(csv_path, index=False, encoding=encoding)
        print(f"[OK] 날짜별 CSV 저장 (Final): {csv_filename} ({len(df)}개)")

        # 이전 날짜 파일들 삭제
        self._cleanup_old_files(csv_path)

        return csv_path

    def get_latest_csv_file(self) -> Optional[Path]:
        """
        final 폴더에서 가장 최근 날짜의 CSV 파일 찾기

        Returns:
            가장 최근 CSV 파일 Path 또는 None
        """
        if not self.final_dir.exists():
            return None

        # {data_name}_YYYYMMDD.csv 패턴의 파일 찾기
        pattern = f"{self.data_name}_*.csv"
        csv_files = list(self.final_dir.glob(pattern))

        if not csv_files:
            return None

        # 날짜별로 정렬 (파일명에서 날짜 추출)
        def extract_date(file_path):
            try:
                # {data_name}_20251208.csv -> 20251208
                date_str = file_path.stem.split('_')[-1]
                return datetime.strptime(date_str, '%Y%m%d')
            except:
                return datetime.min

        csv_files.sort(key=extract_date, reverse=True)
        return csv_files[0]

    def load_from_csv(self, csv_file: Path = None, encoding: str = None) -> pd.DataFrame:
        """
        CSV 파일에서 데이터 로드

        Args:
            csv_file: CSV 파일 경로 (None이면 최신 파일 자동 선택)
            encoding: CSV 인코딩 (None이면 설정값 사용)

        Returns:
            DataFrame
        """
        if encoding is None:
            encoding = ProcessConfig.ENCODING_DEFAULT

        # 파일 경로가 지정되지 않았으면 최신 파일 찾기
        if csv_file is None:
            csv_file = self.get_latest_csv_file()
            if csv_file is None:
                raise FileNotFoundError(f"{self.data_name} CSV 파일을 찾을 수 없습니다")

        df = pd.read_csv(csv_file, encoding=encoding)
        print(f"[OK] CSV에서 {len(df)}개 데이터 로드: {csv_file.name}")
        return df

    def _cleanup_old_files(self, keep_file: Path):
        """
        이전 날짜의 파일들 삭제 (최신 파일만 유지)

        Args:
            keep_file: 유지할 파일 경로
        """
        if not self.final_dir.exists():
            return

        pattern = f"{self.data_name}_*.csv"
        old_files = list(self.final_dir.glob(pattern))
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


def clean_address_for_search(address: str) -> str:
    """
    주소 검색 실패 시 재시도를 위해 주소 정제

    층, 호, 지하, 괄호 등 구체적인 위치 정보를 제거하여 검색 성공률 향상

    예시:
        - "서울특별시 강남구 테헤란로 152 15층 1501호"
          -> "서울특별시 강남구 테헤란로 152"
        - "부산광역시 해운대구 중동 1234 (마린시티)"
          -> "부산광역시 해운대구 중동 1234"

    Args:
        address: 원본 주소 문자열

    Returns:
        정제된 주소 문자열
    """
    if not address or not address.strip():
        return address

    cleaned = address.strip()

    # 괄호 및 괄호 내용 제거 (예: "(마린시티)", "(본관)")
    cleaned = re.sub(r'\([^)]*\)', '', cleaned)

    # 지하 표현 제거 (예: "지하1층", "지하 1층")
    cleaned = re.sub(r'지하\s*\d+\s*층?', '', cleaned)

    # 층 정보 제거 (예: "15층", "3층 301호")
    # 패턴: 숫자 + "층" + (선택적으로 공백 + 숫자 + "호")
    cleaned = re.sub(r'\d+\s*층\s*\d*\s*호?', '', cleaned)

    # 호수만 있는 경우도 제거 (예: "1501호")
    cleaned = re.sub(r'\d+\s*호', '', cleaned)

    # 연속된 공백을 하나로 정리
    cleaned = re.sub(r'\s+', ' ', cleaned)

    # 앞뒤 공백 제거
    cleaned = cleaned.strip()

    return cleaned


def remove_address_duplicates(ctp_nm: str, sig_nm: str, emd_nm: str) -> tuple:
    """
    주소 텍스트 중복 제거

    시군구명에 시도명이 포함되거나, 읍면동명에 상위 주소가 포함되는 현상을 제거

    예시:
        - BAD: ctp_nm="전라남도", sig_nm="전라남도 나주시"
        - GOOD: ctp_nm="전라남도", sig_nm="나주시"

    Args:
        ctp_nm: 시도명
        sig_nm: 시군구명
        emd_nm: 읍면동명

    Returns:
        tuple: (정제된_ctp_nm, 정제된_sig_nm, 정제된_emd_nm)
    """
    # None 또는 빈 문자열 처리
    ctp_nm = str(ctp_nm).strip() if pd.notna(ctp_nm) else ''
    sig_nm = str(sig_nm).strip() if pd.notna(sig_nm) else ''
    emd_nm = str(emd_nm).strip() if pd.notna(emd_nm) else ''

    # 시군구명에서 시도명 제거
    if ctp_nm and sig_nm:
        # "전라남도 나주시" -> "나주시"
        if sig_nm.startswith(ctp_nm):
            sig_nm = sig_nm[len(ctp_nm):].strip()

    # 읍면동명에서 상위 주소 제거
    if emd_nm:
        # 시도명 제거
        if ctp_nm and emd_nm.startswith(ctp_nm):
            emd_nm = emd_nm[len(ctp_nm):].strip()

        # 시군구명 제거 (원본 sig_nm 복원 후 확인)
        full_sig_nm = f"{ctp_nm} {sig_nm}".strip() if ctp_nm else sig_nm
        if full_sig_nm and emd_nm.startswith(full_sig_nm):
            emd_nm = emd_nm[len(full_sig_nm):].strip()
        elif sig_nm and emd_nm.startswith(sig_nm):
            emd_nm = emd_nm[len(sig_nm):].strip()

    return ctp_nm, sig_nm, emd_nm
