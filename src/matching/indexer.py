"""
Elasticsearch 인덱서

고등학교, 대학교, 회사 데이터를 Elasticsearch에 인덱싱합니다.
각 데이터는 geo_point 타입으로 저장되어 지리적 검색이 가능합니다.
"""
import pandas as pd
from elasticsearch import helpers
from pathlib import Path
from config.settings import ElasticsearchConfig, PathConfig
from core.elasticsearch import create_es_client


class ElasticsearchIndexer:
    """Elasticsearch 인덱서"""

    def __init__(self):
        """초기화"""
        self.es = create_es_client()
        self.index_name = ElasticsearchConfig.INDEX_NAME
        self.batch_size = ElasticsearchConfig.BATCH_CONFIG['index_batch_size']

    def create_index_mapping(self):
        """통합 인덱스 매핑 생성"""
        properties = {
            "place_type": {"type": "keyword"},
            "location": {"type": "geo_point"},
            "lat": {"type": "double"},
            "lon": {"type": "double"},
            "ctp_cd": {"type": "keyword"},
            "ctp_nm": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}}
            },
            "sig_cd": {"type": "keyword"},
            "sig_nm": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}}
            },
            "emd_cd": {"type": "keyword"},
            "emd_nm": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}}
            },
            "all_addr_nm": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
            },
            # 고등학교/대학교 필드
            "fac_cd": {"type": "keyword"},
            "fac_nm": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}}
            },
            # 회사 필드
            "corp_cd": {"type": "keyword"},
            "corp_nm": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}}
            },
            "corp_depth1_cd": {"type": "keyword"},
            "corp_depth1": {"type": "keyword"},
            "corp_depth2_cd": {"type": "keyword"},
            "corp_depth2": {"type": "keyword"},
            "corp_depth3_cd": {"type": "keyword"},
            "corp_depth3": {"type": "keyword"},
            "corp_depth4_cd": {"type": "keyword"},
            "corp_depth4": {"type": "keyword"},
            "corp_depth5_cd": {"type": "keyword"},
            "corp_depth5": {"type": "keyword"}
        }

        return {"mappings": {"properties": properties}}

    def create_index(self):
        """인덱스 생성 (기존 인덱스 삭제 후 재생성)"""
        # 기존 인덱스 삭제
        if self.es.indices.exists(index=self.index_name):
            print(f"  - 기존 인덱스 삭제: {self.index_name}")
            self.es.indices.delete(index=self.index_name)

        # 인덱스 생성
        mapping = self.create_index_mapping()
        self.es.indices.create(index=self.index_name, body=mapping)
        print(f"  - 인덱스 생성 완료: {self.index_name}")

    def index_data(self, csv_path, place_type):
        """
        CSV 데이터를 Elasticsearch에 인덱싱

        Args:
            csv_path: CSV 파일 경로
            place_type: 장소 타입 ('high_school', 'university', 'company')

        Returns:
            bool: 성공 여부
        """
        print(f"\n{'='*80}")
        print(f"인덱싱 시작: {place_type}")
        print(f"파일: {csv_path}")
        print(f"{'='*80}")

        # CSV 파일 존재 확인
        if not Path(csv_path).exists():
            print(f"✗ 파일이 존재하지 않습니다: {csv_path}")
            return False

        # CSV 데이터 읽기
        print(f"  - CSV 파일 읽기 중...")
        df = pd.read_csv(csv_path, encoding='utf-8-sig')  # BOM 제거
        total_rows = len(df)
        print(f"  - 읽은 데이터: {total_rows} rows")

        # 데이터 전처리 및 벌크 인덱싱 준비
        def generate_docs():
            for idx, row in df.iterrows():
                # 위경도 좌표
                lat = row['lat']
                lon = row['lon']

                # 좌표 유효성 검사
                if pd.isna(lat) or pd.isna(lon):
                    continue

                # 문서 생성
                doc = {
                    "place_type": place_type,
                    "location": {"lat": float(lat), "lon": float(lon)},
                    "latitude": float(lat),
                    "longitude": float(lon)
                }

                # 모든 컬럼 추가 (NaN 제외)
                for col in df.columns:
                    if col not in ['lat', 'lon', 'latitude', 'longitude']:
                        val = row[col]
                        if pd.notna(val):
                            doc[col] = str(val) if not isinstance(val, (int, float)) else val

                yield {
                    "_index": self.index_name,
                    "_id": f"{place_type}_{idx}",
                    "_source": doc
                }

        # 벌크 인덱싱
        print(f"  - 벌크 인덱싱 시작 (batch_size: {self.batch_size})")
        try:
            success, failed = helpers.bulk(
                self.es,
                generate_docs(),
                chunk_size=self.batch_size,
                request_timeout=300,
                raise_on_error=False
            )
            print(f"  - 인덱싱 완료: {success}개 성공")
            if failed:
                print(f"  - 실패: {failed}개")
            return True
        except Exception as e:
            print(f"✗ 벌크 인덱싱 실패: {e}")
            return False
