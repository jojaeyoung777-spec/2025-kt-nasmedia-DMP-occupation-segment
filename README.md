# 2025_kt 나스미디어 DMP 직업 세그먼트 개발

DMP 시나리오 서비스를 운영하며,  
기존 방식으로는 **사용자 단위의 직업 타겟팅 니즈를 충분히 충족하지 못한다는 한계**를 발견하였습니다.  

이를 개선하기 위해 **위치 정보를 활용한 직업 유추 로직을 End-to-End로 설계·구현**하였으며,  
실제 시나리오에 바로 적용 가능한 **직업 세그먼트 기반을 구축**하여  
사용자 니즈에 부합하는 정교한 타겟팅 옵션을 제공하였습니다.

---

### 티스토리
  - 정리글 
  - https://macbook2.tistory.com/79

---
<img width="1280" height="717" alt="image" src="https://github.com/user-attachments/assets/dac28cf7-b8d8-4ce2-b77c-0312a60c878b" />


# Job Segmentation Pipeline

직장(기업)과 학교(고등학교, 대학교) 위치 데이터를 수집하고, Elasticsearch를 활용하여 DMP 사용자 위치 데이터와 매칭하는 통합 파이프라인입니다.

## 목차

- [프로젝트 개요](#프로젝트-개요)
- [시스템 아키텍처](#시스템-아키텍처)
- [주요 기능](#주요-기능)
- [디렉토리 구조](#디렉토리-구조)
- [설치 및 설정](#설치-및-설정)
- [전체 프로세스 상세 설명](#전체-프로세스-상세-설명)
  - [Phase 0: 데이터 수집 및 전처리](#phase-0-데이터-수집-및-전처리)
  - [Phase 1: Elasticsearch 인덱싱](#phase-1-elasticsearch-인덱싱)
  - [Phase 2: 위치 매칭](#phase-2-위치-매칭)
- [사용 방법](#사용-방법)
- [설정 파일](#설정-파일)
- [데이터 플로우](#데이터-플로우)
- [API 명세](#api-명세)

---

## 프로젝트 개요

**Job Segmentation Pipeline**은 다음의 목적을 위해 설계되었습니다:

1. **데이터 수집**: 공공 API를 통해 기업, 고등학교, 대학교의 위치 정보 및 메타데이터 수집
2. **데이터 전처리**: 주소를 좌표로 변환하고, 법정동코드 및 업종 정보 보강
3. **Elasticsearch 인덱싱**: 수집한 데이터를 지리적 검색이 가능한 Elasticsearch 인덱스에 저장
4. **위치 매칭**: DMP 사용자의 위치(좌표)를 기반으로 가장 가까운 직장/학교를 찾아 매칭

이를 통해 사용자가 어느 학교/직장 근처에 있는지 파악하여 타겟 마케팅, 인구 분석 등에 활용할 수 있습니다.

---

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                     Phase 0: 데이터 수집 및 전처리                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [참조 데이터 수집]                                                │
│    ├─ 업종코드 API (한국무역보험공사) → CSV 저장                      │
│    └─ 법정동코드 API (공공데이터포털) → CSV 저장                       │
│                                                                 │
│  [기업 데이터 파이프라인]                                            │
│    ├─ DART API → Raw 기업 데이터                                  │
│    ├─ 카카오 로컬 API → 주소→좌표, 좌표→법정동 변환                    │
│    ├─ 업종코드 매칭                                                │
│    └─ Final 데이터 생성 (좌표, 법정동, 업종 포함)                      │
│                                                                 │
│  [학교 데이터 파이프라인]                                            │
│    ├─ 안전지도 API → Raw 고등학교/대학교 데이터                        │
│    ├─ 카카오 역지오코딩 → 좌표→법정동 변환                            │
│    └─ Final 데이터 생성 (좌표, 법정동 포함)                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Phase 1: Elasticsearch 인덱싱                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│    ┌──────────────────┐                                         │
│    │  통합 인덱스 생성 │ → job-seg-places                         │
│    └──────────────────┘                                         │
│            ↓                                                    │
│    ┌──────────────────┐                                         │
│    │  벌크 인덱싱      │                                          │
│    │  - 고등학교       │  geo_point 타입으로 저장                   │
│    │  - 대학교        │  (반경 검색 가능)                          │
│    │  - 기업          │                                          │
│    └──────────────────┘                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     Phase 2: 위치 매칭                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [입력]                                                          │
│    DMP 데이터 (adid, lat, lon)                                   │
│                                                                 │
│  [처리]                                                          │
│    ├─ 청크 단위로 데이터 읽기 (50,000개)                            │
│    ├─ 배치 단위로 Elasticsearch geo_distance 쿼리 (1,000개)       │
│    ├─ ThreadPoolExecutor 병렬 처리 (30개 스레드)                   │
│    └─ 가장 가까운 학교/직장 매칭                                     │
│                                                                 │
│  [출력]                                                          │
│    매칭 결과 CSV (adid, 매칭된 학교/직장 정보, 거리)                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 주요 기능

### 1. 다양한 공공 API 연동
- **DART API**: 상장기업 정보 (주소, 업종 등)
- **카카오 로컬 API**: 주소→좌표 변환, 역지오코딩
- **공공데이터포털**: 법정동코드 전체 목록
- **한국무역보험공사 API**: 업종코드 5단계 계층 구조
- **안전지도 API**: 전국 고등학교/대학교 위치

### 2. 강력한 데이터 전처리
- 주소 정제 및 재시도 로직 (층/호 제거)
- 병렬 처리를 통한 대용량 데이터 처리 (ThreadPoolExecutor)
- API 실패 시 Fallback 메커니즘
- 날짜별 버전 관리 (자동으로 이전 파일 삭제)

### 3. Elasticsearch 기반 지리적 검색
- geo_point 타입으로 좌표 저장
- geo_distance 쿼리로 반경 검색
- place_type별 검색 반경 설정 (고등학교: 200m, 대학교: 300m, 기업: 200m)
- msearch를 활용한 배치 검색으로 성능 최적화

### 4. 대용량 데이터 처리 최적화
- 청크 단위 파일 읽기 (50,000 레코드)
- 배치 단위 Elasticsearch 쿼리 (1,000 레코드)
- 병렬 처리 (30개 스레드 동시 실행)
- 중간 저장 기능 (100,000개마다 자동 저장)

---

## 디렉토리 구조

```
job_seg/
├── config/                       # 설정 파일 (프로젝트 레벨)
│   ├── __init__.py
│   └── settings.py               # 레거시 설정 (사용 안 함)
│
├── src/                          # 소스 코드
│   ├── main.py                   # 메인 진입점 (전체 파이프라인 실행)
│   │
│   ├── config/                   # 실제 사용하는 설정
│   │   ├── __init__.py
│   │   └── settings.py           # API 키, 경로, Elasticsearch 설정
│   │
│   ├── core/                     # 핵심 유틸리티
│   │   ├── __init__.py
│   │   ├── elasticsearch.py      # Elasticsearch 클라이언트 팩토리
│   │   ├── logging.py            # 로깅 설정
│   │   └── utils.py              # 공통 유틸리티 (Fallback, 주소 정제 등)
│   │
│   ├── data_pipeline/            # 데이터 수집 및 전처리
│   │   ├── __init__.py
│   │   ├── pipeline.py           # 파이프라인 오케스트레이터
│   │   │
│   │   ├── api_clients/          # API 클라이언트
│   │   │   ├── __init__.py
│   │   │   └── kakao_api.py      # 카카오 로컬 API 클라이언트
│   │   │
│   │   ├── collectors/           # 데이터 수집기
│   │   │   ├── __init__.py
│   │   │   ├── collect_dart_data.py          # 기업 데이터 수집
│   │   │   ├── collect_high_schools.py       # 고등학교 데이터 수집
│   │   │   ├── collect_universities.py       # 대학교 데이터 수집
│   │   │   ├── collect_industry_codes.py     # 업종코드 수집
│   │   │   └── collect_legal_dong_codes.py   # 법정동코드 수집
│   │   │
│   │   └── processors/           # 데이터 전처리기
│   │       ├── __init__.py
│   │       ├── add_coordinates.py                    # 좌표 및 법정동 추가
│   │       ├── add_industry_classification.py        # 업종코드 매칭
│   │       ├── enrich_with_kakao_api.py             # 카카오 API 보강
│   │       └── match_school_legal_dong_codes.py     # 학교 법정동 매칭
│   │
│   ├── matching/                 # Elasticsearch 매칭
│   │   ├── __init__.py
│   │   ├── indexer.py            # Elasticsearch 인덱서
│   │   └── matcher.py            # 위치 매칭 엔진
│   │
│   └── data/                     # 데이터 디렉토리
│       ├── raw/                  # API에서 수집한 원본 데이터
│       ├── intermediate/         # 중간 처리 데이터
│       ├── final/                # 최종 전처리 데이터 (인덱싱 대상)
│       ├── cache/                # 캐시 데이터
│       └── dmp/                  # DMP 입력 데이터 (매칭 대상)
│
├── output/                       # 매칭 결과 출력
├── notebook/                     # Jupyter 노트북
├── test/                         # 테스트 코드
├── requirements.txt              # Python 의존성
└── README.md                     # 이 문서
```

---

## 설치 및 설정

### 1. 가상환경 생성 및 활성화

```bash
cd /home/jaeyoung/projects/job_seg
python3 -m venv .venv
source .venv/bin/activate
```

### 2. 의존성 설치

```bash
pip install -r requirements.txt
```

주요 라이브러리:
- `requests`: HTTP API 호출
- `pandas`: 데이터 처리
- `elasticsearch`: Elasticsearch 연동
- `pyproj`: 좌표 변환 (Web Mercator ↔ WGS84)
- `tqdm`: 진행률 표시

### 3. API 키 설정

`src/config/settings.py` 파일을 열어 다음 API 키를 설정합니다:

```python
class APIConfig:
    DART_API_KEY = "your_dart_api_key"
    KAKAO_API_KEY = "your_kakao_rest_api_key"
    LEGAL_DONG_API_KEY = "your_public_data_portal_key"
    SAFEMAP_SERVICE_KEY = "your_safemap_key"
```

### 4. Elasticsearch 연결 설정

`src/config/settings.py`의 Elasticsearch 설정을 확인합니다:

```python
class ElasticsearchConfig:
    ES_CONFIG = {
        'host': '10.10.20.61',
        'port': 19200,
        'user': 'admin',
        'password': 'your_password',
        'ca_certs': '/opt/elastic/elasticsearch/config/certs/http_ca.crt',
    }
```

---

## 전체 프로세스 상세 설명

### Phase 0: 데이터 수집 및 전처리

데이터 수집 및 전처리는 `data_pipeline/pipeline.py`의 `DataPipeline` 클래스가 담당합니다.

#### Step 0-1: 참조 데이터 수집 (선행 필수)

참조 데이터는 이후 단계에서 매칭에 사용됩니다.

**업종코드 수집** (`collectors/collect_industry_codes.py`):
```python
# 한국무역보험공사 API 호출
# 전체 업종코드 다운로드 (페이징 처리)
# → raw/업종코드_raw_YYYYMMDD.csv

# 5단계 계층 구조로 변환
# - Depth 1: 대분류 (A~U 알파벳)
# - Depth 2~5: 중분류, 소분류, 세분류, 세세분류
# → final/업종코드_YYYYMMDD.csv
```

**법정동코드 수집** (`collectors/collect_legal_dong_codes.py`):
```python
# 공공데이터포털 API 호출
# 전국 법정동코드 전체 다운로드
# → raw/법정동코드_raw_YYYYMMDD.csv

# 필요 컬럼만 선택 (법정동코드, 법정동명)
# → final/법정동코드_YYYYMMDD.csv
```

#### Step 0-2: 기업 데이터 파이프라인

**1. DART API로 상장기업 데이터 수집** (`collectors/collect_dart_data.py`):
```python
# Step 1: DART에서 전체 기업 코드 목록 다운로드 (ZIP)
# Step 2: 상장 기업만 필터링 (stock_code 존재)
# Step 3: 각 기업의 상세 정보 수집 (병렬 처리)
#   - 병렬 처리: ThreadPoolExecutor (5개 스레드)
#   - 재시도: 최대 3회
#   - Rate limiting: 0.3초 대기
# → raw/기업위치_raw_YYYYMMDD.csv

# 수집 정보: 기업명, 주소, 업종코드, CEO, 설립일 등
```

**2. 카카오 API로 좌표 및 법정동 변환** (`processors/add_coordinates.py`):
```python
# 주소 → 좌표 변환 (카카오 로컬 API)
#   - 1차 시도: 원본 주소
#   - 2차 시도: 정제된 주소 (층/호/지하/괄호 제거)
#
# 좌표 → 법정동 변환 (역지오코딩)
#   - ctp_cd, ctp_nm (시도)
#   - sig_cd, sig_nm (시군구)
#   - emd_cd, emd_nm (읍면동)
#
# 병렬 처리: ThreadPoolExecutor (3개 스레드)
# → intermediate/companies_with_coordinates.csv
```

**3. 업종코드 매칭** (`processors/add_industry_classification.py`):
```python
# 기업의 induty_code를 업종코드 CSV와 매칭
# 5단계 계층 구조 정보 추가:
#   - corp_depth1_cd, corp_depth1 (대분류)
#   - corp_depth2_cd, corp_depth2 (중분류)
#   - ...
#   - corp_depth5_cd, corp_depth5 (세세분류)
#
# 주소 중복 제거 적용 (예: "전라남도 나주시" → "나주시")
# → final/기업위치_YYYYMMDD.csv
```

#### Step 0-3: 학교 데이터 파이프라인

**1. 안전지도 API로 학교 데이터 수집**:

- **고등학교** (`collectors/collect_high_schools.py`):
  ```python
  # 안전지도 API IF_0035 (학교 - 초,중,고,기타)
  # 페이징 처리로 전체 데이터 수집
  # "고등학교" 또는 "고교" 포함된 것만 필터링
  # → raw/고등학교_raw_YYYYMMDD.csv
  ```

- **대학교** (`collectors/collect_universities.py`):
  ```python
  # 안전지도 API IF_0034 (대학교)
  # 좌표 변환: Web Mercator → WGS84 (pyproj)
  # → raw/대학교_raw_YYYYMMDD.csv
  ```

**2. 역지오코딩으로 법정동 매칭** (`processors/match_school_legal_dong_codes.py`):
```python
# 좌표 기반 역지오코딩 (카카오 API)
# 좌표 → 법정동코드 변환
#   - ctp_cd, ctp_nm (시도)
#   - sig_cd, sig_nm (시군구)
#   - emd_cd, emd_nm (읍면동)
#
# 주소 중복 제거 적용
#
# 대학교 전용: "대학원" 제외 필터링
#
# → final/고등학교_YYYYMMDD.csv
# → final/대학교_YYYYMMDD.csv
```

#### 데이터 흐름 요약

```
[API 수집]
    ↓
[raw/ 디렉토리 저장]
    ↓
[전처리: 좌표 변환, 법정동 매칭, 업종 매칭]
    ↓
[intermediate/ 디렉토리 저장] (기업만)
    ↓
[최종 데이터 생성]
    ↓
[final/ 디렉토리 저장]
```

**날짜별 버전 관리**:
- 모든 파일명에 날짜 포함 (예: `기업위치_20251230.csv`)
- 새 파일 저장 시 이전 날짜 파일 자동 삭제
- Fallback: API 실패 시 최신 파일 사용

---

### Phase 1: Elasticsearch 인덱싱

인덱싱은 `matching/indexer.py`의 `ElasticsearchIndexer` 클래스가 담당합니다.

#### 인덱스 생성

```python
# 통합 인덱스: job-seg-places
# - 고등학교, 대학교, 기업 데이터를 하나의 인덱스에 저장
# - place_type 필드로 구분 ('high_school', 'university', 'company')
```

**매핑 구조**:
```json
{
  "mappings": {
    "properties": {
      "place_type": {"type": "keyword"},
      "location": {"type": "geo_point"},
      "lat": {"type": "double"},
      "lon": {"type": "double"},

      "ctp_cd": {"type": "keyword"},
      "ctp_nm": {"type": "keyword"},
      "sig_cd": {"type": "keyword"},
      "sig_nm": {"type": "keyword"},
      "emd_cd": {"type": "keyword"},
      "emd_nm": {"type": "keyword"},

      "fac_cd": {"type": "keyword"},
      "fac_nm": {"type": "keyword"},

      "corp_cd": {"type": "keyword"},
      "corp_nm": {"type": "keyword"},
      "corp_depth1_cd": {"type": "keyword"},
      "corp_depth1": {"type": "keyword"}
    }
  }
}
```

#### 벌크 인덱싱

```python
# 각 place_type별로 final/ 디렉토리에서 최신 CSV 읽기
# 벌크 인덱싱 (배치 크기: 5,000개)
# 좌표 유효성 검사 (lat/lon이 NaN인 경우 제외)

# 예시:
indexer.index_data('final/고등학교_20251230.csv', 'high_school')
indexer.index_data('final/대학교_20251230.csv', 'university')
indexer.index_data('final/기업위치_20251230.csv', 'company')
```

---

### Phase 2: 위치 매칭

위치 매칭은 `matching/matcher.py`의 `SyncMatcher` 클래스가 담당합니다.

#### 입력 데이터

DMP 데이터 (`src/data/dmp/` 디렉토리):
```csv
adid,lat,lon,time_type
user123,37.5665,126.9780,DAY
user456,35.1796,129.0756,NIGHT
...
```

**전처리**:
- `time_type='DAY'` 필터링 (회사 매칭의 경우)
- `lat`, `lon`이 결측치인 행 제거

#### 매칭 알고리즘

```python
# 청크 단위로 파일 읽기 (50,000개씩)
for chunk in pd.read_csv(input_csv, chunksize=50000):

    # 배치 단위로 Elasticsearch msearch 쿼리 (1,000개씩)
    for batch in split_into_batches(chunk, 1000):

        # Elasticsearch geo_distance 쿼리
        query = {
            "bool": {
                "must": [
                    {"term": {"place_type": "high_school"}},
                    {"geo_distance": {
                        "distance": "200m",
                        "location": {"lat": user_lat, "lon": user_lon}
                    }}
                ]
            }
        }

        # 거리순 정렬하여 가장 가까운 1개만 반환
        # → 매칭 결과 수집
```

#### 병렬 처리

```python
# ThreadPoolExecutor로 병렬 처리 (30개 스레드)
# 각 스레드가 독립적으로 배치 처리
# I/O bound 작업에 최적화

with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {
        executor.submit(process_batch, batch): batch
        for batch in batches
    }

    # as_completed로 완료된 배치부터 순차적으로 결과 수집
    for future in as_completed(futures):
        results.extend(future.result())
```

#### 중간 저장

```python
# 100,000개마다 중간 저장
# 메모리 사용량 제어 및 진행 상황 보존
if len(results) >= 100000:
    save_to_csv(results, output_csv, mode='append')
    results = []
```

#### 출력 데이터

매칭 결과 (`output/` 디렉토리):
```csv
adid,lat,lon,distance,fac_cd,ctp_cd,sig_cd,emd_cd
user123,37.5665,126.9780,85.3,123456,1100000000,1168000000,1168010100
user456,35.1796,129.0756,142.7,789012,2600000000,2614000000,2614010300
...
```

**필드 설명**:
- `adid`: 사용자 ID
- `lat`, `lon`: 사용자 위치 좌표
- `distance`: 매칭된 장소까지의 거리 (미터)
- `fac_cd` / `corp_cd`: 매칭된 학교/기업 코드
- `ctp_cd`, `sig_cd`, `emd_cd`: 법정동코드 (시도, 시군구, 읍면동)

---

## 사용 방법

### 전체 파이프라인 실행

```bash
# 1. 전체 파이프라인 (데이터 수집 + 인덱싱 + 매칭)
python src/main.py --mode full

# 2. 데이터 수집만 실행
python src/main.py --mode collect

# 3. 인덱싱만 실행
python src/main.py --mode index

# 4. 매칭만 실행 (기존 인덱스 사용)
python src/main.py --mode match

# 5. 데이터 수집 건너뛰기 (기존 데이터 사용)
python src/main.py --mode full --skip-collect

# 6. 인덱싱 건너뛰기 (기존 Elasticsearch 인덱스 사용)
python src/main.py --mode full --skip-index
```

### 개별 모듈 실행

#### 데이터 수집

```bash
# 참조 데이터 수집
python src/data_pipeline/collectors/collect_industry_codes.py
python src/data_pipeline/collectors/collect_legal_dong_codes.py

# 기업 데이터 수집
python src/data_pipeline/collectors/collect_dart_data.py
python src/data_pipeline/processors/add_coordinates.py
python src/data_pipeline/processors/add_industry_classification.py

# 학교 데이터 수집
python src/data_pipeline/collectors/collect_high_schools.py
python src/data_pipeline/collectors/collect_universities.py
python src/data_pipeline/processors/match_school_legal_dong_codes.py
```

#### 데이터 파이프라인만 실행

```bash
# 전체 데이터 파이프라인
python src/data_pipeline/pipeline.py --mode full

# 참조 데이터만
python src/data_pipeline/pipeline.py --mode reference

# 기업 데이터만
python src/data_pipeline/pipeline.py --mode company

# 학교 데이터만
python src/data_pipeline/pipeline.py --mode school
```

---

## 설정 파일

`src/config/settings.py`에서 모든 설정을 관리합니다.

### APIConfig - API 키 및 엔드포인트

```python
class APIConfig:
    # DART (전자공시) API
    DART_API_KEY = "your_api_key"
    DART_BASE_URL = "https://opendart.fss.or.kr/api"

    # 카카오 로컬 API
    KAKAO_API_KEY = "your_api_key"
    KAKAO_BASE_URL = "https://dapi.kakao.com/v2/local"

    # 공공데이터포털 - 법정동코드 API
    LEGAL_DONG_API_KEY = "your_api_key"
    LEGAL_DONG_API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"

    # 한국무역보험공사 - 업종코드 API
    INDUSTRY_API_URL = "https://api.odcloud.kr/api/15064297/v1/..."

    # 안전지도 API
    SAFEMAP_SERVICE_KEY = "your_api_key"
    SAFEMAP_HIGH_SCHOOL_URL = "http://safemap.go.kr/openapi2/IF_0035"
    SAFEMAP_UNIVERSITY_URL = "http://safemap.go.kr/openapi2/IF_0034"
```

### ElasticsearchConfig - Elasticsearch 설정

```python
class ElasticsearchConfig:
    ES_CONFIG = {
        'host': '10.10.20.61',
        'port': 19200,
        'user': 'admin',
        'password': 'your_password',
        'ca_certs': '/path/to/http_ca.crt',
    }

    INDEX_NAME = 'job-seg-places'

    # 검색 반경 설정 (place_type별)
    GEO_SEARCH_CONFIG = {
        'high_school': '200m',
        'university': '300m',
        'company': '200m',
    }

    # 배치 처리 설정
    BATCH_CONFIG = {
        'index_batch_size': 5000,   # 인덱싱 배치 크기
        'search_batch_size': 1000,  # 검색 배치 크기
        'max_workers': 4,            # 병렬 처리 워커 수
    }
```

### PathConfig - 경로 설정

```python
class PathConfig:
    DATA_DIR = PROJECT_ROOT / "src" / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"            # API 원본 데이터
    INTERMEDIATE_DATA_DIR = DATA_DIR / "intermediate"  # API 중간 처리 데이터
    FINAL_DATA_DIR = DATA_DIR / "final"        # API 최종 전처리 데이터 (인덱싱 대상)
    CACHE_DIR = DATA_DIR / "cache"             # 캐시
    DMP_DATA_DIR = DATA_DIR / "dmp"            # DMP 후보군
    OUTPUT_DIR = PROJECT_ROOT / "output"       # 매칭 결과
```

### ProcessConfig - 데이터 처리 설정

```python
class ProcessConfig:
    ENCODING_DEFAULT = "utf-8-sig"

    # API 호출 설정
    API_REQUEST_DELAY = 0.3      # 초 (rate limiting)
    API_TIMEOUT = 30             # 초
    API_RETRY_COUNT = 3          # 재시도 횟수

    # DART API 전용
    DART_API_REQUEST_DELAY = 0.3
    DART_MAX_WORKERS = 5

    # 병렬 처리 설정
    MAX_WORKERS = 3              # ThreadPoolExecutor 워커 수
    USE_PARALLEL = True

    # Fallback 설정
    USE_FALLBACK = True          # API 실패 시 기존 파일 사용 여부
```

### MatchingConfig - 매칭 설정

```python
class MatchingConfig:
    CONCURRENCY = 30             # 동시 처리 배치 수
    CHUNK_SIZE = 50000           # CSV 읽기 청크 크기
    INTERMEDIATE_SAVE_INTERVAL = 100000  # 중간 저장 간격
    REQUEST_TIMEOUT = 30         # ES 요청 타임아웃 (초)
    MAX_RETRIES = 3              # 최대 재시도 횟수
```

---

## 데이터 플로우

### 1. 기업 데이터

```
DART API
   ↓ (collect_dart_data.py)
raw/기업위치_raw_YYYYMMDD.csv
   ↓ (add_coordinates.py)
intermediate/companies_with_coordinates.csv
   ↓ (add_industry_classification.py)
final/기업위치_YYYYMMDD.csv
   ↓ (indexer.py)
Elasticsearch (place_type='company')
   ↓ (matcher.py)
output/company_matched.csv
```

**최종 컬럼**:
- `corp_cd`, `corp_nm`: 기업 코드, 기업명
- `ctp_cd`, `ctp_nm`: 시도 코드, 시도명
- `sig_cd`, `sig_nm`: 시군구 코드, 시군구명
- `emd_cd`, `emd_nm`: 읍면동 코드, 읍면동명
- `all_addr_nm`: 전체 주소
- `lat`, `lon`: 위도, 경도
- `corp_depth1_cd`, `corp_depth1`: 대분류 코드, 명칭
- `corp_depth2_cd`, `corp_depth2`: 중분류 코드, 명칭
- `corp_depth3_cd`, `corp_depth3`: 소분류 코드, 명칭
- `corp_depth4_cd`, `corp_depth4`: 세분류 코드, 명칭
- `corp_depth5_cd`, `corp_depth5`: 세세분류 코드, 명칭

### 2. 고등학교 데이터

```
안전지도 API (IF_0035)
   ↓ (collect_high_schools.py)
raw/고등학교_raw_YYYYMMDD.csv
   ↓ (match_school_legal_dong_codes.py)
final/고등학교_YYYYMMDD.csv
   ↓ (indexer.py)
Elasticsearch (place_type='high_school')
   ↓ (matcher.py)
output/high_school_matched.csv
```

**최종 컬럼**:
- `fac_cd`, `fac_nm`: 시설 코드, 시설명
- `ctp_cd`, `ctp_nm`: 시도 코드, 시도명
- `sig_cd`, `sig_nm`: 시군구 코드, 시군구명
- `emd_cd`, `emd_nm`: 읍면동 코드, 읍면동명
- `all_addr_nm`: 전체 주소
- `lat`, `lon`: 위도, 경도

### 3. 대학교 데이터

```
안전지도 API (IF_0034)
   ↓ (collect_universities.py)
raw/대학교_raw_YYYYMMDD.csv
   ↓ (match_school_legal_dong_codes.py)
final/대학교_YYYYMMDD.csv
   ↓ (indexer.py)
Elasticsearch (place_type='university')
   ↓ (matcher.py)
output/university_matched.csv
```

**최종 컬럼**: 고등학교와 동일

---

## API 명세

### 1. DART API (전자공시)

**기업 코드 목록 다운로드**:
```
GET https://opendart.fss.or.kr/api/corpCode.xml
Parameters:
  - crtfc_key: API 키
Response: ZIP 파일 (CORPCODE.xml 포함)
```

**기업 상세 정보 조회**:
```
GET https://opendart.fss.or.kr/api/company.json
Parameters:
  - crtfc_key: API 키
  - corp_code: 기업 코드
Response: JSON (기업명, 주소, 업종코드 등)
```

### 2. 카카오 로컬 API

**주소 → 좌표 변환**:
```
GET https://dapi.kakao.com/v2/local/search/address.json
Headers:
  - Authorization: KakaoAK {REST_API_KEY}
Parameters:
  - query: 주소 문자열
Response: JSON (x=경도, y=위도)
```

**좌표 → 법정동 변환 (역지오코딩)**:
```
GET https://dapi.kakao.com/v2/local/geo/coord2regioncode.json
Headers:
  - Authorization: KakaoAK {REST_API_KEY}
Parameters:
  - x: 경도
  - y: 위도
  - input_coord: WGS84
Response: JSON (region_type='B'에서 법정동 정보)
```

### 3. 공공데이터포털 - 법정동코드 API

```
GET https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList
Parameters:
  - ServiceKey: API 키
  - type: json
  - pageNo: 페이지 번호
  - numOfRows: 페이지당 행 수
  - flag: Y
Response: JSON (법정동코드, 법정동명)
```

### 4. 한국무역보험공사 - 업종코드 API

```
GET https://api.odcloud.kr/api/15064297/v1/uddi:07a7ea27-b8b2-4698-835c-c3b950cefb34
Parameters:
  - serviceKey: API 키
  - page: 페이지 번호
  - perPage: 페이지당 행 수
  - returnType: JSON
Response: JSON (업종코드, 업종한글명, 원본업종코드)
```

### 5. 안전지도 API

**고등학교 (IF_0035)**:
```
GET http://safemap.go.kr/openapi2/IF_0035
Parameters:
  - serviceKey: API 키
  - pageNo: 페이지 번호
  - numOfRows: 페이지당 행 수
  - returnType: XML
Response: XML (fcltycd, fcltynm, lnmadr, latitude, longitude)
```

**대학교 (IF_0034)**:
```
GET http://safemap.go.kr/openapi2/IF_0034
Parameters:
  - serviceKey: API 키
  - pageNo: 페이지 번호
  - numOfRows: 페이지당 행 수
  - returnType: XML
Response: XML (fclty_nm, adres, rn_adres, x, y)
```

---

## 트러블슈팅

### 1. API 호출 실패

**증상**: `[ERROR] API 호출 실패`

**해결 방법**:
- API 키 확인 (`src/config/settings.py`)
- 네트워크 연결 확인
- API 사용량 제한 확인 (rate limiting)
- Fallback 모드 활성화 (`USE_FALLBACK = True`)

### 2. Elasticsearch 연결 실패

**증상**: `Connection refused` 또는 `Timeout`

**해결 방법**:
```python
# Elasticsearch 서버 상태 확인
curl -u admin:password https://10.10.20.61:19200/_cluster/health

# 설정 확인
- host, port, user, password 검증
- ca_certs 경로 확인
- 방화벽 설정 확인
```

### 3. 메모리 부족

**증상**: `MemoryError` 또는 시스템이 느려짐

**해결 방법**:
```python
# 청크 크기 조정
MatchingConfig.CHUNK_SIZE = 25000  # 기본 50000에서 감소

# 병렬 처리 워커 수 감소
MatchingConfig.CONCURRENCY = 15  # 기본 30에서 감소

# 중간 저장 간격 감소
MatchingConfig.INTERMEDIATE_SAVE_INTERVAL = 50000  # 기본 100000에서 감소
```

### 4. 좌표 변환 실패

**증상**: 많은 데이터의 `lat`, `lon`이 NaN

**해결 방법**:
- 주소 데이터 품질 확인
- 카카오 API 키 및 사용량 확인
- 재시도 로직 확인 (`API_RETRY_COUNT`)
- 수동으로 보정 스크립트 실행:
  ```bash
  python src/data_pipeline/processors/enrich_with_kakao_api.py
  ```

---

## 성능 최적화

### 현재 설정 (권장)

```python
# 데이터 수집
DART_MAX_WORKERS = 5         # DART API 병렬 처리
MAX_WORKERS = 3               # 카카오 API 병렬 처리

# Elasticsearch
BATCH_CONFIG = {
    'index_batch_size': 5000,
    'search_batch_size': 1000,
}

# 매칭
CONCURRENCY = 30              # 동시 처리 배치 수
CHUNK_SIZE = 50000            # CSV 읽기 청크 크기
```

### 성능 튜닝 팁

1. **더 빠른 인덱싱**:
   - `index_batch_size` 증가 (5000 → 10000)
   - Elasticsearch 클러스터 리소스 증설

2. **더 빠른 매칭**:
   - `CONCURRENCY` 증가 (30 → 50)
   - `search_batch_size` 증가 (1000 → 2000)
   - SSD 사용 (I/O 병목 해소)

3. **메모리 절약**:
   - `CHUNK_SIZE` 감소 (50000 → 25000)
   - `INTERMEDIATE_SAVE_INTERVAL` 감소


---
