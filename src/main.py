"""
Job Segmentation 통합 파이프라인

통합된 데이터 수집, 인덱싱, 매칭 파이프라인의 진입점
"""
import argparse
from config.settings import validate_config, PathConfig
from matching.indexer import ElasticsearchIndexer
from matching.matcher import SyncMatcher
from data_pipeline.pipeline import DataPipeline
from core.logging import setup_logging


class JobSegPipeline:
    """통합 파이프라인 오케스트레이터"""

    def __init__(self):
        """초기화"""
        # 설정 검증
        validate_config()

        # 로깅 설정
        setup_logging()

        # 모듈 초기화
        self.data_pipeline = DataPipeline()
        self.indexer = ElasticsearchIndexer()
        self.matcher = SyncMatcher()

    def run_data_collection(self):
        """
        Phase 0: 데이터 수집 및 전처리

        API를 통해 기업/학교 데이터를 수집하고 전처리합니다.
        """
        print("\n" + "="*80)
        print("PHASE 0: 데이터 수집 및 전처리")
        print("="*80)

        self.data_pipeline.run_full_pipeline()

        print("\n" + "="*80)
        print("데이터 수집 및 전처리 완료!")
        print("="*80)

    def run_indexing(self):
        """
        Phase 1: Elasticsearch 인덱싱

        최종 데이터(final/)를 Elasticsearch에 인덱싱합니다.
        """
        print("\n" + "="*80)
        print("PHASE 1: Elasticsearch 인덱싱")
        print("="*80)

        # 인덱스 생성
        self.indexer.create_index()

        # 최종 데이터 디렉토리에서 파일 찾기
        final_dir = PathConfig.FINAL_DATA_DIR

        # 고등학교 데이터 인덱싱
        high_school_files = list(final_dir.glob("고등학교*.csv"))
        if high_school_files:
            latest_high = max(high_school_files, key=lambda p: p.stem.split('_')[-1])
            print(f"\n고등학교 데이터 인덱싱: {latest_high.name}")
            self.indexer.index_data(str(latest_high), 'high_school')
        else:
            print("\n⚠ 고등학교 데이터 파일이 없습니다")

        # 대학교 데이터 인덱싱
        university_files = list(final_dir.glob("대학교*.csv"))
        if university_files:
            latest_univ = max(university_files, key=lambda p: p.stem.split('_')[-1])
            print(f"\n대학교 데이터 인덱싱: {latest_univ.name}")
            self.indexer.index_data(str(latest_univ), 'university')
        else:
            print("\n⚠ 대학교 데이터 파일이 없습니다")

        # 기업 데이터 인덱싱
        company_files = list(final_dir.glob("기업위치*.csv"))
        if company_files:
            latest_company = max(company_files, key=lambda p: p.stem.split('_')[-1])
            print(f"\n기업 데이터 인덱싱: {latest_company.name}")
            self.indexer.index_data(str(latest_company), 'company')
        else:
            print("\n⚠ 기업 데이터 파일이 없습니다")

        print("\n" + "="*80)
        print("인덱싱 완료!")
        print("="*80)

    def run_matching(self):
        """
        Phase 2: 위치 매칭 (동기 + 병렬)

        DMP 데이터(_adid.csv)를 Elasticsearch와 매칭합니다.
        """
        print("\n" + "="*80)
        print("PHASE 2: 위치 매칭 (동기 + 병렬)")
        print("="*80)

        # 모든 매칭 작업 실행
        self.matcher.run_all_matching_jobs()

        print("\n" + "="*80)
        print("매칭 완료!")
        print("="*80)

    def run_full_pipeline(self, skip_collect=False, skip_index=False):
        """
        전체 파이프라인 실행

        Args:
            skip_collect: 데이터 수집 건너뛰기 (기존 데이터 사용)
            skip_index: 인덱싱 건너뛰기 (기존 인덱스 사용)
        """
        print("\n" + "="*80)
        print("Job Segmentation 통합 파이프라인 시작")
        print("="*80)

        if not skip_collect:
            # Phase 0: 데이터 수집 및 전처리
            self.run_data_collection()

        if not skip_index:
            # Phase 1: 인덱싱
            self.run_indexing()

        # Phase 2: 매칭
        self.run_matching()

        print("\n" + "="*80)
        print("전체 파이프라인 완료!")
        print("="*80)


def main():
    """CLI 진입점"""
    parser = argparse.ArgumentParser(
        description='Job Segmentation 통합 파이프라인',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 전체 파이프라인 실행 (데이터 수집 + 인덱싱 + 매칭)
  python src/main.py --mode full

  # 데이터 수집만 실행
  python src/main.py --mode collect

  # 인덱싱만 실행
  python src/main.py --mode index

  # 매칭만 실행 (기존 인덱스 사용)
  python src/main.py --mode match

  # 데이터 수집 건너뛰기 (기존 데이터 사용)
  python src/main.py --mode full --skip-collect

  # 인덱싱 건너뛰기 (기존 인덱스 사용)
  python src/main.py --mode full --skip-index
        """
    )

    parser.add_argument(
        '--mode',
        choices=['collect', 'index', 'match', 'full'],
        default='full',
        help='실행 모드 선택'
    )

    parser.add_argument(
        '--skip-collect',
        action='store_true',
        help='데이터 수집 건너뛰기 (기존 데이터 사용)'
    )

    parser.add_argument(
        '--skip-index',
        action='store_true',
        help='인덱싱 건너뛰기 (기존 ES 인덱스 사용)'
    )

    args = parser.parse_args()

    # 파이프라인 실행
    pipeline = JobSegPipeline()

    if args.mode == 'collect':
        # 데이터 수집만
        pipeline.run_data_collection()

    elif args.mode == 'index':
        # 인덱싱만
        pipeline.run_indexing()

    elif args.mode == 'match':
        # 매칭만
        pipeline.run_matching()

    elif args.mode == 'full':
        # 전체 파이프라인
        pipeline.run_full_pipeline(
            skip_collect=args.skip_collect,
            skip_index=args.skip_index
        )


if __name__ == "__main__":
    main()
