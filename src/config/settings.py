"""
통합 설정 파일 - API, Elasticsearch, 경로 등 전역 설정 관리

API 데이터 수집 및 Elasticsearch 매칭의 모든 설정을 통합 관리합니다.
"""
from pathlib import Path

# 프로젝트 루트 디렉토리 (src/config/ 기준)
PROJECT_ROOT = Path(__file__).parent.parent.parent

# ========================================
# API 설정
# ========================================
class APIConfig:
    """API 키 및 엔드포인트 관리"""

    # DART (전자공시) API - 기업 데이터
    DART_API_KEY = "0"
    DART_BASE_URL = "https://opendart.fss.or.kr/api"

    # 카카오 로컬 API - 좌표 변환 및 주소 검색
    KAKAO_API_KEY = "0"
    KAKAO_BASE_URL = "https://dapi.kakao.com/v2/local"

    # 공공데이터포털 - 법정동코드 API
    LEGAL_DONG_API_KEY = "0+IkkzWvTkWmUY6KTNiPMMnHo3fIMcJYDw=="
    LEGAL_DONG_API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"

    # 한국무역보험공사 - 업종코드 API
    INDUSTRY_API_URL = "0

    # 안전지도 API - 학교 데이터
    SAFEMAP_SERVICE_KEY = "0"
    SAFEMAP_HIGH_SCHOOL_URL = "0"  # 학교(초,중,고,기타)
    SAFEMAP_UNIVERSITY_URL = "0"  # 대학교


# ========================================
# Elasticsearch 설정
# ========================================
class ElasticsearchConfig:
    """Elasticsearch 연결 및 검색 설정"""

    # Elasticsearch 연결 정보
    ES_CONFIG = {
        'host': '00.00.00.0',
        'port': 00,
        'user': '0',
        'password': '0',
        'ca_certs': '/opt/elastic/elasticsearch/config/certs/http_ca.crt',
    }

    # 인덱스 이름
    INDEX_NAME = 'job-seg-places'

    # Geo 검색 설정 (place_type별)
    GEO_SEARCH_CONFIG = {
        'high_school': '200m',  # 고등학교 검색 반경
        'university': '300m',   # 대학교 검색 반경
        'company': '200m',      # 직장 검색 반경
    }

    # 배치 처리 설정
    BATCH_CONFIG = {
        'index_batch_size': 5000,  # 인덱싱 배치 크기
        'search_batch_size': 1000,  # 검색 배치 크기
        'max_workers': 4,  # 병렬 처리 워커 수
    }


# ========================================
# 경로 설정
# ========================================
class PathConfig:
    """파일 경로 관리"""

    # 데이터 디렉토리
    DATA_DIR = PROJECT_ROOT / "src" / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    INTERMEDIATE_DATA_DIR = DATA_DIR / "intermediate"
    FINAL_DATA_DIR = DATA_DIR / "final"
    CACHE_DIR = DATA_DIR / "cache"
    DMP_DATA_DIR = DATA_DIR / "dmp"  # DMP 입력 데이터

    # 소스 코드
    SRC_DIR = PROJECT_ROOT / "src"

    # 출력 디렉토리 (매칭 결과)
    OUTPUT_DIR = PROJECT_ROOT / "output"


# ========================================
# 출력 파일명 설정
# ========================================
class OutputConfig:
    """출력 파일명 관리"""

    # Raw 데이터
    RAW_DART_COMPANIES = "dart_companies_raw.csv"
    RAW_HIGH_SCHOOLS = "high_schools_raw.csv"
    RAW_UNIVERSITIES = "universities_raw.csv"

    # Intermediate 데이터 (기업)
    INTERMEDIATE_WITH_COORDINATES = "companies_with_coordinates.csv"
    INTERMEDIATE_WITH_INDUSTRY = "companies_with_industry.csv"
    INTERMEDIATE_WITH_LEGAL_DONG = "companies_with_legal_dong.csv"


# ========================================
# 처리 설정
# ========================================
class ProcessConfig:
    """데이터 처리 설정"""

    # 인코딩
    ENCODING_DEFAULT = "utf-8-sig"
    ENCODING_EUCKR = "cp949"

    # API 호출 설정
    API_REQUEST_DELAY = 0.3  # 초 (일반 API rate limiting 회피)
    API_TIMEOUT = 30  # 초
    API_RETRY_COUNT = 3  # 재시도 횟수

    # DART API 전용 설정
    DART_API_REQUEST_DELAY = 0.3  # 초 (DART API 딜레이)
    DART_MAX_WORKERS = 5  # DART 병렬 처리 스레드 수

    # 배치 처리 크기
    BATCH_SIZE = 1000  # 대용량 배치 처리

    # 병렬 처리 설정 (카카오 등 일반 API용)
    MAX_WORKERS = 3  # 동시 실행 스레드 수
    USE_PARALLEL = True  # 병렬 처리 사용 여부

    # 좌표 변환
    COORD_SOURCE_CRS = "EPSG:3857"  # Web Mercator
    COORD_TARGET_CRS = "EPSG:4326"  # WGS84 (위경도)

    # 캐시 유효 기간 (일)
    CACHE_VALID_DAYS = 30

    # Fallback 설정
    USE_FALLBACK = True  # API 실패 시 기존 파일 사용 여부


# ========================================
# 매칭 설정
# ========================================
class MatchingConfig:
    """Elasticsearch 매칭 설정"""

    # 병렬 처리 설정
    CONCURRENCY = 30  # 동시 처리 배치 수 (ThreadPoolExecutor max_workers)

    # 청크 처리 크기
    CHUNK_SIZE = 50000  # CSV 읽기 청크 크기 (50,000 레코드)

    # 중간 저장 간격
    INTERMEDIATE_SAVE_INTERVAL = 100000  # 100,000 레코드마다 저장

    # 타임아웃 및 재시도
    REQUEST_TIMEOUT = 30  # ES 요청 타임아웃 (초)
    MAX_RETRIES = 3  # 최대 재시도 횟수


# ========================================
# 로깅 설정
# ========================================
class LogConfig:
    """로깅 설정"""
    LOG_FORMAT = "[%(asctime)s] %(levelname)s - %(message)s"
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LOG_LEVEL = "INFO"


# ========================================
# 설정 검증
# ========================================
def validate_config():
    """설정 검증 및 필수 디렉토리 생성"""
    # 필수 디렉토리 생성
    for path in [
        PathConfig.RAW_DATA_DIR,
        PathConfig.INTERMEDIATE_DATA_DIR,
        PathConfig.FINAL_DATA_DIR,
        PathConfig.CACHE_DIR,
        PathConfig.DMP_DATA_DIR,
        PathConfig.OUTPUT_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)

    print("[OK] 설정 검증 완료")
    print(f"  - 프로젝트 루트: {PROJECT_ROOT}")
    print(f"  - Data 폴더: {PathConfig.DATA_DIR}")
    print(f"  - DMP 데이터 폴더: {PathConfig.DMP_DATA_DIR}")
    print(f"  - 출력 폴더: {PathConfig.OUTPUT_DIR}")

    return True


if __name__ == "__main__":
    # 설정 테스트
    validate_config()
    print(f"\nAPI 엔드포인트:")
    print(f"  - DART: {APIConfig.DART_BASE_URL}")
    print(f"  - Kakao: {APIConfig.KAKAO_BASE_URL}")
    print(f"  - Legal Dong: {APIConfig.LEGAL_DONG_API_URL}")
    print(f"  - Industry: {APIConfig.INDUSTRY_API_URL}")
    print(f"  - SafeMap High School: {APIConfig.SAFEMAP_HIGH_SCHOOL_URL}")
    print(f"  - SafeMap University: {APIConfig.SAFEMAP_UNIVERSITY_URL}")

    print(f"\nElasticsearch 설정:")
    print(f"  - Host: {ElasticsearchConfig.ES_CONFIG['host']}:{ElasticsearchConfig.ES_CONFIG['port']}")
    print(f"  - Index: {ElasticsearchConfig.INDEX_NAME}")
    print(f"  - Search Distance (high_school): {ElasticsearchConfig.GEO_SEARCH_CONFIG['high_school']}")
    print(f"  - Search Distance (university): {ElasticsearchConfig.GEO_SEARCH_CONFIG['university']}")
    print(f"  - Search Distance (company): {ElasticsearchConfig.GEO_SEARCH_CONFIG['company']}")

    print(f"\n매칭 설정:")
    print(f"  - Concurrency: {MatchingConfig.CONCURRENCY}")
    print(f"  - Chunk Size: {MatchingConfig.CHUNK_SIZE:,}")
    print(f"  - Intermediate Save Interval: {MatchingConfig.INTERMEDIATE_SAVE_INTERVAL:,}")

