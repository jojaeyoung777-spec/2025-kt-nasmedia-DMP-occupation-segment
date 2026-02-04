"""
Elasticsearch Multi-search 기반 위치 매칭 (동기 + 병렬 처리)

ThreadPoolExecutor를 사용한 병렬 처리:
- 동기 코드로 단순화
- ThreadPoolExecutor로 여러 배치 동시 처리
- I/O bound 작업에 최적화
"""
import pandas as pd
import requests
import json
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from config.settings import ElasticsearchConfig, MatchingConfig, PathConfig
from core.elasticsearch import get_es_url


class SyncMatcher:
    """동기 매칭 엔진 (병렬 실행)"""

    def __init__(self):
        """초기화"""
        self.es_config = ElasticsearchConfig.ES_CONFIG
        self.index_name = ElasticsearchConfig.INDEX_NAME
        self.distance_config = ElasticsearchConfig.GEO_SEARCH_CONFIG
        self.batch_size = ElasticsearchConfig.BATCH_CONFIG['search_batch_size']
        self.concurrency = MatchingConfig.CONCURRENCY
        self.chunk_size = MatchingConfig.CHUNK_SIZE
        self.timeout = MatchingConfig.REQUEST_TIMEOUT
        self.max_retries = MatchingConfig.MAX_RETRIES

        # ES URL 및 인증
        self.es_url = get_es_url()
        self.auth = (self.es_config['user'], self.es_config['password'])

    def get_distance(self, place_type):
        """place_type에 따른 검색 반경 반환"""
        return self.distance_config.get(place_type, '300m')

    def sync_msearch(self, search_body):
        """
        동기 msearch 요청 (재시도 포함)

        Args:
            search_body: msearch 요청 본문 (리스트)

        Returns:
            응답 JSON 또는 None
        """
        url = f"{self.es_url}/_msearch"

        # msearch는 newline-delimited JSON 형식
        body = ""
        for item in search_body:
            body += json.dumps(item) + "\n"

        headers = {"Content-Type": "application/x-ndjson"}

        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    url,
                    data=body,
                    headers=headers,
                    auth=self.auth,
                    timeout=self.timeout,
                    verify=False  # SSL 검증 비활성화
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    print(f"⚠ ES 오류 (status {response.status_code}): {response.text[:200]}")
                    if attempt < self.max_retries - 1:
                        continue
                    return None

            except Exception as e:
                print(f"⚠ 요청 오류 (시도 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    continue
                return None

        return None

    def process_batch(self, batch_data):
        """
        단일 배치 처리 (ThreadPoolExecutor에서 병렬 실행됨)

        Args:
            batch_data: (batch_idx, batch_locations, place_type) 튜플

        Returns:
            매칭 결과 리스트
        """
        batch_idx, batch, place_type = batch_data
        distance = self.get_distance(place_type)

        # msearch 요청 본문 구성
        search_body = []
        for user_lat, user_lon, adid in batch:
            search_body.append({"index": self.index_name})
            search_body.append({
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"place_type": place_type}},
                            {
                                "geo_distance": {
                                    "distance": distance,
                                    "location": {"lat": user_lat, "lon": user_lon}
                                }
                            }
                        ]
                    }
                },
                "sort": [{
                    "_geo_distance": {
                        "location": {"lat": user_lat, "lon": user_lon},
                        "order": "asc",
                        "unit": "m"
                    }
                }],
                "size": 1,
                "_source": True
            })

        # 동기 msearch 실행
        response = self.sync_msearch(search_body)

        if not response or 'responses' not in response:
            return []

        # 응답 처리
        batch_results = []
        for idx, resp in enumerate(response['responses']):
            if 'error' in resp or resp['hits']['total']['value'] == 0:
                continue

            user_lat, user_lon, adid = batch[idx]
            hit = resp['hits']['hits'][0]
            source = hit['_source']

            result = {
                'adid': adid,
                'lat': user_lat,
                'lon': user_lon,
                'distance': hit['sort'][0]
            }

            if place_type in ['high_school', 'university']:
                result.update({
                    'fac_cd': source.get('fac_cd'),
                    'ctp_cd': source.get('ctp_cd'),
                    'sig_cd': source.get('sig_cd'),
                    'emd_cd': source.get('emd_cd')
                })
            else:
                result.update({
                    'corp_cd': source.get('corp_cd'),
                    'corp_depth1_cd': source.get('corp_depth1_cd'),
                    'corp_depth2_cd': source.get('corp_depth2_cd'),
                    'corp_depth3_cd': source.get('corp_depth3_cd'),
                    'corp_depth4_cd': source.get('corp_depth4_cd'),
                    'corp_depth5_cd': source.get('corp_depth5_cd'),
                    'ctp_cd': source.get('ctp_cd'),
                    'sig_cd': source.get('sig_cd'),
                    'emd_cd': source.get('emd_cd')
                })

            batch_results.append(result)

        return batch_results

    def batch_find_nearest_places_parallel(self, place_type, user_locations):
        """
        병렬로 배치 단위 검색 (ThreadPoolExecutor 사용)

        Args:
            place_type: 장소 타입
            user_locations: 사용자 위치 리스트 [(lat, lon, adid), ...]

        Returns:
            매칭 결과 리스트
        """
        results = []

        # 배치들 생성
        batches = []
        for i in range(0, len(user_locations), self.batch_size):
            batch = user_locations[i:i+self.batch_size]
            batches.append((i, batch, place_type))

        # ThreadPoolExecutor로 병렬 처리
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            # 모든 배치 작업 제출
            futures = {executor.submit(self.process_batch, batch_data): batch_data
                      for batch_data in batches}

            # 진행률 표시하며 결과 수집
            with tqdm(total=len(batches), desc="  배치 처리 (병렬)") as pbar:
                for future in as_completed(futures):
                    batch_results = future.result()
                    results.extend(batch_results)
                    pbar.update(1)

        return results

    def match_locations(self, place_type, input_csv, output_csv):
        """
        위치 매칭 (동기 + 병렬)

        Args:
            place_type: 'high_school', 'university', 'company'
            input_csv: 입력 CSV 경로
            output_csv: 출력 CSV 경로
        """
        distance = self.get_distance(place_type)
        print(f"\n{'='*80}")
        print(f"매칭 시작: {place_type} (동기 + 병렬 처리)")
        print(f"입력 파일: {input_csv}")
        print(f"출력 파일: {output_csv}")
        print(f"검색 반경: {distance}")
        print(f"배치 크기: {self.batch_size}개씩")
        print(f"병렬 처리: {self.concurrency}개 스레드 🚀")
        print(f"{'='*80}")

        if not Path(input_csv).exists():
            print(f"✗ 파일이 존재하지 않습니다: {input_csv}")
            return False

        try:
            print(f"\n사용자 데이터 처리 중...")

            all_results = []
            processed_count = 0
            matched_count = 0
            first_write = True  # 첫 저장 여부 추적

            try:
                total_lines = sum(1 for _ in open(input_csv)) - 1
                print(f"전체 데이터: {total_lines:,}행")
            except:
                total_lines = None

            chunk_num = 0

            for user_chunk in pd.read_csv(input_csv, chunksize=self.chunk_size):
                chunk_num += 1

                # 전처리
                if place_type == 'company' and 'time_type' in user_chunk.columns:
                    before_count = len(user_chunk)
                    user_chunk = user_chunk[user_chunk['time_type'] == 'DAY']
                    if len(user_chunk) == 0:
                        continue
                    print(f"  청크 {chunk_num}: time_type='DAY' 필터링 ({before_count:,} → {len(user_chunk):,}행)")

                user_chunk = user_chunk.dropna(subset=['lat', 'lon'])
                if len(user_chunk) == 0:
                    continue

                print(f"\n청크 {chunk_num}: {len(user_chunk):,}행 처리 중...")

                # 사용자 위치 리스트
                user_locations = [
                    (row['lat'], row['lon'], row['adid'])
                    for _, row in user_chunk.iterrows()
                ]

                # 병렬 배치 검색
                chunk_results = self.batch_find_nearest_places_parallel(
                    place_type,
                    user_locations
                )

                all_results.extend(chunk_results)
                processed_count += len(user_locations)
                matched_count += len(chunk_results)

                print(f"  매칭 완료: {len(chunk_results):,}/{len(user_locations):,}개 (누적: {matched_count:,}/{processed_count:,})")

                # 중간 저장 (100,000개마다)
                if len(all_results) >= MatchingConfig.INTERMEDIATE_SAVE_INTERVAL:
                    if first_write:
                        self._save_results(all_results, output_csv, mode='w', header=True)
                        first_write = False
                    else:
                        self._save_results(all_results, output_csv, mode='a', header=False)
                    all_results = []

            # 남은 결과 저장
            if all_results:
                if first_write:
                    self._save_results(all_results, output_csv, mode='w', header=True)
                else:
                    self._save_results(all_results, output_csv, mode='a', header=False)

            print(f"\n{'='*80}")
            print(f"매칭 완료!")
            print(f"  - 처리: {processed_count:,}개")
            print(f"  - 매칭 성공: {matched_count:,}개")
            print(f"  - 매칭률: {100*matched_count/processed_count:.1f}%")
            print(f"  - 출력 파일: {output_csv}")
            print(f"{'='*80}")

            return True

        except Exception as e:
            print(f"\n✗ 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _save_results(self, results, output_csv, mode='w', header=True):
        """결과 저장"""
        if not results:
            return

        df = pd.DataFrame(results)
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_csv, index=False, mode=mode, header=header, encoding='utf-8-sig')
        if mode == 'w' or header:
            print(f"  결과 저장: {output_csv} ({len(df):,}개)")
        else:
            print(f"  중간 저장: {len(df):,}개 추가")

    def run_all_matching_jobs(self):
        """
        모든 매칭 작업 실행

        DMP 데이터(_adid.csv)를 사용하여 매칭
        """
        matching_jobs = [
            
            {
                'input': str(PathConfig.DMP_DATA_DIR / 'high_adid.csv'),
                'output': str(PathConfig.OUTPUT_DIR / 'high_school_matched.csv'),
                'type': 'high_school'
            },
            
            #{
            #    'input': str(PathConfig.DMP_DATA_DIR / 'univ_adid.csv'),
            #    'output': str(PathConfig.OUTPUT_DIR / 'university_matched.csv'),
            #    'type': 'university'
            #},
            
            #{
            #   'input': str(PathConfig.DMP_DATA_DIR / 'work_adid.csv'),
            #    'output': str(PathConfig.OUTPUT_DIR / 'company_matched.csv'),
            #    'type': 'company'
            #}
        ]

        for job in matching_jobs:
            self.match_locations(
                place_type=job['type'],
                input_csv=job['input'],
                output_csv=job['output']
            )
