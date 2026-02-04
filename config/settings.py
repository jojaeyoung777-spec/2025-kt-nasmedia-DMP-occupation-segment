"""
통합 설정 파일 - API 키, 경로, 상수 관리

Clean Architecture 원칙에 따라 설정을 계층별로 분리하여 관리합니다.
"""
from pathlib import Path
from datetime import datetime


# ========================================
# 프로젝트 루트 디렉토리
# ========================================
PROJECT_ROOT = Path(__file__).parent.parent


# ========================================
# API 설정
# ========================================
class APIConfig:
    """API 키 및 엔드포인트 관리"""

    # DART (전자공시) API - 기업 데이터
    DART_API_KEY = "deed5f1ba69b1cda2f3b2f7819061503b57f6f5a"
    DART_BASE_URL = "https://opendart.fss.or.kr/api"

    # 카카오 로컬 API - 좌표 변환 및 주소 검색
    KAKAO_API_KEY = "b223172563ee609c326539aa7e724f97"
    KAKAO_BASE_URL = "https://dapi.kakao.com/v2/local"

    # 공공데이터포털 - 법정동코드 API
    LEGAL_DONG_API_KEY = "1igRNIkRXyPHQIcbfUsrTtnQ2JEiPGuFhqZnkny2onlEGDxdgUcfx+IkkzWvTkWmUY6KTNiPMMnHo3fIMcJYDw=="
    LEGAL_DONG_API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"

    # 한국무역보험공사 - 업종코드 API
    INDUSTRY_API_KEY = "1igRNIkRXyPHQIcbfUsrTtnQ2JEiPGuFhqZnkny2onlEGDxdgUcfx+IkkzWvTkWmUY6KTNiPMMnHo3fIMcJYDw=="
    INDUSTRY_API_URL = "https://api.odcloud.kr/api/15064297/v1/uddi:07a7ea27-b8b2-4698-835c-c3b950cefb34"

    # 안전지도 API - 학교 데이터
    SAFEMAP_SERVICE_KEY = "83E6CJ1F-83E6-83E6-83E6-83E6CJ1FCY"
    SAFEMAP_HIGH_SCHOOL_URL = "http://safemap.go.kr/openapi2/IF_0035"  # 학교(초,중,고,기타)
    SAFEMAP_UNIVERSITY_URL = "http://safemap.go.kr/openapi2/IF_0034"  # 대학교


# ========================================
# 경로 설정
# ========================================
class PathConfig:
    """파일 경로 관리 (Clean Architecture)"""

    # 소스 코드
    SRC_DIR = PROJECT_ROOT / "src"

    # 설정 파일
    CONFIG_DIR = PROJECT_ROOT / "config"

    # 스크립트
    SCRIPTS_DIR = PROJECT_ROOT / "scripts"

    # 테스트
    TEST_DIR = PROJECT_ROOT / "test"

    # Reference 디렉토리 (CSV 캐싱 및 폴백용)
    REFERENCE_DIR = PROJECT_ROOT / "reference"

    # Notebooks
    NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

    # 출력 디렉토리
    OUTPUT_DIR = PROJECT_ROOT / "output"

    @staticmethod
    def get_reference_file_pattern(data_type: str) -> str:
        """
        Reference 파일 패턴 반환

        Args:
            data_type: 'legal_dong', 'industry', 'company', 'high_school', 'university'

        Returns:
            파일명 패턴 (예: "법정동코드_*.csv")
        """
        patterns = {
            'legal_dong': '법정동코드_*.csv',
            'industry': '업종코드_*.csv',
            'company': '기업위치_*.csv',
            'high_school': '고등학교_*.csv',
            'university': '대학교_*.csv'
        }
        return patterns.get(data_type, f'{data_type}_*.csv')

    @staticmethod
    def get_reference_filename(data_type: str, date: datetime = None) -> str:
        """
        Reference 파일명 생성 (날짜별)

        Args:
            data_type: 'legal_dong', 'industry', 'company', 'high_school', 'university'
            date: 날짜 (None이면 오늘 날짜)

        Returns:
            파일명 (예: "법정동코드_20251210.csv")
        """
        if date is None:
            date = datetime.now()

        date_str = date.strftime('%Y%m%d')

        names = {
            'legal_dong': f'법정동코드_{date_str}.csv',
            'industry': f'업종코드_{date_str}.csv',
            'company': f'기업위치_{date_str}.csv',
            'high_school': f'고등학교_{date_str}.csv',
            'university': f'대학교_{date_str}.csv'
        }
        return names.get(data_type, f'{data_type}_{date_str}.csv')


# ========================================
# 처리 설정
# ========================================
class ProcessConfig:
    """데이터 처리 설정"""

    # 인코딩
    ENCODING_DEFAULT = "utf-8-sig"
    ENCODING_EUCKR = "cp949"

    # API 호출 설정
    API_REQUEST_DELAY = 0.2  # 초 (API rate limiting 회피)
    API_TIMEOUT = 30  # 초
    API_RETRY_COUNT = 3  # 재시도 횟수
    API_RETRY_DELAY = 1.0  # 재시도 대기 시간 (초)

    # 배치 처리 크기
    BATCH_SIZE = 1000  # 대용량 배치 처리

    # 병렬 처리 설정
    MAX_WORKERS = 3  # 동시 실행 스레드 수
    USE_PARALLEL = True  # 병렬 처리 사용 여부

    # 좌표 변환
    COORD_SOURCE_CRS = "EPSG:3857"  # Web Mercator
    COORD_TARGET_CRS = "EPSG:4326"  # WGS84 (위경도)

    # 캐시 설정
    CACHE_VALID_DAYS = 30  # 캐시 유효 기간 (일)

    # Fallback 설정
    USE_FALLBACK = True  # API 실패 시 기존 CSV 파일 사용 여부

    # 데이터 검증 설정
    VALIDATE_COORDINATES = True  # 좌표 유효성 검증
    VALIDATE_LEGAL_DONG_CODE = True  # 법정동코드 유효성 검증

    # 주소 정제 설정
    REMOVE_BUILDING_INFO = True  # 건물명/층수 제거
    REMOVE_PARENTHESES = True  # 괄호 제거

    # 좌표 유효성 범위 (대한민국)
    KOREA_LAT_MIN = 33.0
    KOREA_LAT_MAX = 43.0
    KOREA_LON_MIN = 124.0
    KOREA_LON_MAX = 132.0


# ========================================
# 로깅 설정
# ========================================
class LogConfig:
    """로깅 설정"""
    LOG_FORMAT = "[%(asctime)s] %(levelname)s - %(message)s"
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LOG_LEVEL = "INFO"
    LOG_TO_FILE = False  # 파일 로깅 사용 여부
    LOG_FILE_PATH = PROJECT_ROOT / "logs" / "pipeline.log"


# ========================================
# 설정 검증
# ========================================
def validate_config():
    """
    설정 검증 및 필수 디렉토리 생성

    Returns:
        True if validation succeeds

    Raises:
        Exception if validation fails
    """
    # 필수 디렉토리 생성
    required_dirs = [
        PathConfig.SRC_DIR,
        PathConfig.CONFIG_DIR,
        PathConfig.SCRIPTS_DIR,
        PathConfig.TEST_DIR,
        PathConfig.REFERENCE_DIR,
        PathConfig.OUTPUT_DIR,
    ]

    if LogConfig.LOG_TO_FILE:
        required_dirs.append(LogConfig.LOG_FILE_PATH.parent)

    for path in required_dirs:
        path.mkdir(parents=True, exist_ok=True)

    # API 키 검증
    missing_keys = []
    if not APIConfig.DART_API_KEY:
        missing_keys.append("DART_API_KEY")
    if not APIConfig.KAKAO_API_KEY:
        missing_keys.append("KAKAO_API_KEY")
    if not APIConfig.LEGAL_DONG_API_KEY:
        missing_keys.append("LEGAL_DONG_API_KEY")

    if missing_keys:
        print(f"[WARNING] 누락된 API 키: {', '.join(missing_keys)}")
        print("[INFO] Fallback 모드로 동작합니다 (기존 CSV 파일 사용)")

    print("[OK] 설정 검증 완료")
    print(f"  - 프로젝트 루트: {PROJECT_ROOT}")
    print(f"  - Reference 폴더: {PathConfig.REFERENCE_DIR}")
    print(f"  - Fallback 모드: {'활성화' if ProcessConfig.USE_FALLBACK else '비활성화'}")

    return True


if __name__ == "__main__":
    # 설정 테스트
    validate_config()

    print(f"\n[API 엔드포인트]")
    print(f"  - DART: {APIConfig.DART_BASE_URL}")
    print(f"  - Kakao: {APIConfig.KAKAO_BASE_URL}")
    print(f"  - Legal Dong: {APIConfig.LEGAL_DONG_API_URL}")
    print(f"  - Industry: {APIConfig.INDUSTRY_API_URL}")
    print(f"  - SafeMap High School: {APIConfig.SAFEMAP_HIGH_SCHOOL_URL}")
    print(f"  - SafeMap University: {APIConfig.SAFEMAP_UNIVERSITY_URL}")

    print(f"\n[Reference 파일 관리]")
    print(f"  - 법정동코드: {PathConfig.get_reference_filename('legal_dong')}")
    print(f"  - 업종코드: {PathConfig.get_reference_filename('industry')}")
    print(f"  - 기업위치: {PathConfig.get_reference_filename('company')}")
    print(f"  - 고등학교: {PathConfig.get_reference_filename('high_school')}")
    print(f"  - 대학교: {PathConfig.get_reference_filename('university')}")
