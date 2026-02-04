# Data Pipeline 아키텍처

## 개요

데이터 수집 및 전처리 파이프라인입니다. API로부터 데이터를 수집하여 CSV로 저장하고, 이를 전처리하여 최종 데이터를 생성합니다.

## 디렉토리 구조

```
data_pipeline/
├── collectors/              # 데이터 수집 (API → CSV)
│   ├── collect_dart_data.py            # DART API - 상장기업 정보
│   ├── collect_high_schools.py         # SafeMap API - 고등학교 정보
│   ├── collect_universities.py         # SafeMap API - 대학교 정보
│   ├── collect_industry_codes.py       # 한국무역보험공사 API - 업종코드
│   └── collect_legal_dong_codes.py     # 공공데이터포털 API - 법정동코드
│
├── processors/              # 데이터 전처리 (CSV → CSV)
│   ├── add_coordinates.py              # 카카오 API로 좌표 변환
│   ├── add_legal_dong_codes.py         # 법정동코드 매칭
│   ├── add_industry_classification.py  # 업종코드 매칭
│   └── match_school_legal_dong_codes.py # 학교 법정동코드 매칭
│
├── api_clients/             # 재사용 가능한 API 클라이언트
│   └── kakao_api.py                    # 카카오 로컬 API (실시간 호출)
│
└── pipeline.py              # 파이프라인 오케스트레이터
```

## 아키텍처 원칙

### 1. Collectors vs Processors

- **Collectors**: API에서 데이터를 수집하여 CSV로 저장
  - 역할: 원천 데이터 수집
  - 입력: API 엔드포인트
  - 출력: CSV 파일 (`data/raw/`, `data/final/`)

- **Processors**: CSV 데이터를 읽어서 전처리 후 다시 CSV로 저장
  - 역할: 데이터 보강, 변환, 매칭
  - 입력: CSV 파일
  - 출력: CSV 파일 (`data/intermediate/`, `data/final/`)

### 2. API Clients vs Collectors

- **API Clients** (`api_clients/`): 실시간 API 호출이 필요한 경우
  - 예: 카카오 API (주소 → 좌표 변환, 검색 등)
  - 특징: 매번 다른 파라미터로 호출

- **Collectors** (`collectors/`): 데이터를 한 번 수집하여 재사용
  - 예: 업종코드, 법정동코드
  - 특징: 전체 데이터를 다운로드하여 CSV로 저장

## 데이터 흐름

### 참조 데이터 (선행 필수)

```
1. collect_industry_codes.py
   → API: 한국무역보험공사
   → 출력: data/final/업종코드_YYYYMMDD.csv

2. collect_legal_dong_codes.py
   → API: 공공데이터포털
   → 출력: data/final/법정동코드_YYYYMMDD.csv
```

### 기업 데이터 파이프라인

```
1. collect_dart_data.py
   → API: DART
   → 출력: data/raw/dart_companies_raw.csv

2. add_coordinates.py
   → 입력: dart_companies_raw.csv
   → API: 카카오 (주소 → 좌표)
   → 출력: data/intermediate/companies_with_coordinates.csv

3. add_legal_dong_codes.py
   → 입력: companies_with_coordinates.csv
   → 참조: 법정동코드_YYYYMMDD.csv
   → 출력: data/intermediate/companies_with_legal_dong.csv

4. add_industry_classification.py
   → 입력: companies_with_legal_dong.csv
   → 참조: 업종코드_YYYYMMDD.csv
   → 출력: data/final/기업위치_YYYYMMDD.csv
```

### 학교 데이터 파이프라인

```
1. collect_high_schools.py
   → API: SafeMap
   → 출력: data/raw/high_schools_raw.csv

2. collect_universities.py
   → API: SafeMap
   → 출력: data/raw/universities_raw.csv

3. match_school_legal_dong_codes.py
   → 입력: high_schools_raw.csv, universities_raw.csv
   → 참조: 법정동코드_YYYYMMDD.csv
   → API: 카카오 (대학교 주소 보정)
   → 출력: data/final/고등학교_YYYYMMDD.csv, data/final/대학교_YYYYMMDD.csv
```

## 실행 방법

### 전체 파이프라인 실행

```bash
cd src/data_pipeline
python pipeline.py
```

### 모드별 실행

```bash
# 참조 데이터만 수집 (업종코드, 법정동코드)
python pipeline.py --mode reference

# 기업 데이터만 처리
python pipeline.py --mode company

# 학교 데이터만 처리
python pipeline.py --mode school

# 전체 파이프라인
python pipeline.py --mode full
```

### 개별 스크립트 실행

```bash
# 참조 데이터
python collectors/collect_industry_codes.py
python collectors/collect_legal_dong_codes.py

# 기업 데이터
python collectors/collect_dart_data.py
python processors/add_coordinates.py
python processors/add_legal_dong_codes.py
python processors/add_industry_classification.py

# 학교 데이터
python collectors/collect_high_schools.py
python collectors/collect_universities.py
python processors/match_school_legal_dong_codes.py
```

## Fallback 메커니즘

모든 Collector는 Fallback을 지원합니다:

1. **정상 수집**: API 호출 성공 → 날짜별 CSV 저장 → 이전 파일 자동 삭제
2. **API 실패**: Fallback 활성화 시 → `data/final/` 폴더의 최신 CSV 사용
3. **Fallback 없음**: 에러 발생 → 사용자 개입 필요

설정: `config/settings.py` → `ProcessConfig.USE_FALLBACK = True`

## 파일 버전 관리

날짜별로 파일이 생성되며, 최신 파일만 유지됩니다:

```
data/final/
├── 업종코드_20251211.csv          # 최신
├── 법정동코드_20251211.csv        # 최신
├── 기업위치_20251211.csv          # 최신
├── 고등학교_20251211.csv          # 최신
└── 대학교_20251211.csv            # 최신
```

이전 날짜의 파일은 자동으로 삭제됩니다.

## 설정

`config/settings.py`에서 다음을 설정할 수 있습니다:

- **API 키**: `APIConfig` 클래스
- **경로**: `PathConfig` 클래스
- **처리 옵션**: `ProcessConfig` 클래스
  - `USE_FALLBACK`: API 실패 시 기존 파일 사용 여부
  - `API_TIMEOUT`: API 타임아웃 (초)
  - `API_RETRY_COUNT`: 재시도 횟수
  - `MAX_WORKERS`: 병렬 처리 워커 수

## 리팩토링 히스토리

### 2025-12-11: Collectors와 API Clients 분리

**변경 이유**: 아키텍처 일관성 확보

**Before**:
- `api_clients/industry_api.py` (클래스 기반)
- `api_clients/legal_dong_api.py` (클래스 기반)
- `processors/`에서 API 클라이언트를 직접 import하여 사용

**After**:
- `collectors/collect_industry_codes.py` (함수 기반)
- `collectors/collect_legal_dong_codes.py` (함수 기반)
- `processors/`는 수집된 CSV 파일만 읽어서 사용

**장점**:
- 아키텍처 일관성: 모든 API 수집은 collectors에서 담당
- 의존성 단순화: processors는 CSV만 읽으면 됨
- 재실행 가능: API 없이도 processors 단독 실행 가능
- 캐싱 효과: 참조 데이터를 한 번만 수집하고 재사용
