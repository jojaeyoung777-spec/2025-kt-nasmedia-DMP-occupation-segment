"""
Data Pipeline 오케스트레이터

데이터 수집 및 전처리 파이프라인을 통합 실행합니다.
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))


class DataPipeline:
    """데이터 수집 및 전처리 파이프라인"""

    def __init__(self):
        """초기화"""
        pass

    def run_reference_data_collection(self):
        """
        참조 데이터 수집 (선행 필수)

        업종코드와 법정동코드를 API에서 수집합니다.
        이 데이터는 processors에서 매칭에 사용됩니다.
        """
        print("\n" + "="*80)
        print("참조 데이터 수집 시작")
        print("="*80)

        # Step 1: 업종코드 수집
        print("\n[Step 1] 한국무역보험공사 API로 업종코드 수집")
        from data_pipeline.collectors.collect_industry_codes import main as collect_industry
        try:
            collect_industry()
        except Exception as e:
            print(f"⚠ 업종코드 수집 실패 (Fallback 사용): {e}")

        # Step 2: 법정동코드 수집
        print("\n[Step 2] 공공데이터포털 API로 법정동코드 수집")
        from data_pipeline.collectors.collect_legal_dong_codes import main as collect_legal_dong
        try:
            collect_legal_dong()
        except Exception as e:
            print(f"⚠ 법정동코드 수집 실패 (Fallback 사용): {e}")

        print("\n" + "="*80)
        print("참조 데이터 수집 완료")
        print("="*80)

    def run_company_pipeline(self):
        """
        기업 데이터 파이프라인

        0. DART API로 상장기업 데이터 수집 → raw 저장
        1. raw 데이터 로드 (API 실패 시 최신 raw 사용)
        2. 카카오 API로 좌표 및 법정동 변환 → intermediate 저장
        3. 업종코드 매칭 및 최종 CSV 생성 → final 저장
        3.1. 전처리 실패 시 → final 최신 파일 사용
        """
        print("\n" + "="*80)
        print("기업 데이터 파이프라인 시작")
        print("="*80)

        # Step 0: DART 기업 데이터 수집 (raw 저장)
        print("\n[Step 0] DART API로 상장기업 데이터 수집 → raw 저장")
        from data_pipeline.collectors.collect_dart_data import main as collect_dart
        try:
            collect_dart()  # API 성공/실패와 관계없이 raw 파일 존재
        except Exception as e:
            print(f"⚠ DART 수집 실패 (최신 raw 사용): {e}")

        # Step 1-3: 무조건 전처리 수행 (raw 데이터가 있으면)
        # Step 1: 좌표 및 법정동 변환
        print("\n[Step 1] 카카오 API로 좌표 및 법정동 변환 → intermediate 저장")
        from data_pipeline.processors.add_coordinates import main as add_coords
        try:
            add_coords()
        except Exception as e:
            print(f"⚠ 좌표 변환 실패: {e}")
            print("[INFO] 전처리 실패, final 폴더의 최신 파일을 매칭에 사용합니다.")

        # Step 2: 업종코드 매칭
        print("\n[Step 2] 업종코드 매칭 및 최종 CSV 생성 → final 저장")
        from data_pipeline.processors.add_industry_classification import main as add_industry
        try:
            add_industry()
        except Exception as e:
            print(f"⚠ 업종코드 매칭 실패: {e}")
            print("[INFO] 전처리 실패, final 폴더의 최신 파일을 매칭에 사용합니다.")

        print("\n" + "="*80)
        print("기업 데이터 파이프라인 완료")
        print("="*80)

    def run_school_pipeline(self):
        """
        학교 데이터 파이프라인

        0. 안전지도 API로 고등학교/대학교 데이터 수집 → raw 저장
        1. raw 데이터 로드 (API 실패 시 최신 raw 사용)
        2. 역지오코딩으로 법정동코드 매칭 → final 저장
        2.1. 전처리 실패 시 → final 최신 파일 사용
        """
        print("\n" + "="*80)
        print("학교 데이터 파이프라인 시작")
        print("="*80)

        # Step 0-1: 학교 데이터 수집 (raw 저장)
        print("\n[Step 0] 안전지도 API로 고등학교 데이터 수집 → raw 저장")
        from data_pipeline.collectors.collect_high_schools import main as collect_high
        try:
            collect_high()  # API 성공/실패와 관계없이 raw 파일 존재
        except Exception as e:
            print(f"⚠ 고등학교 수집 실패 (최신 raw 사용): {e}")

        print("\n[Step 1] 안전지도 API로 대학교 데이터 수집 → raw 저장")
        from data_pipeline.collectors.collect_universities import main as collect_univ
        try:
            collect_univ()  # API 성공/실패와 관계없이 raw 파일 존재
        except Exception as e:
            print(f"⚠ 대학교 수집 실패 (최신 raw 사용): {e}")

        # Step 2: 무조건 전처리 수행 (raw 데이터가 있으면)
        print("\n[Step 2] 학교 법정동코드 매칭 (역지오코딩) → final 저장")
        from data_pipeline.processors.match_school_legal_dong_codes import main as match_codes
        try:
            match_codes()
        except Exception as e:
            print(f"⚠ 법정동코드 매칭 실패: {e}")
            print("[INFO] 전처리 실패, final 폴더의 최신 파일을 매칭에 사용합니다.")

        print("\n" + "="*80)
        print("학교 데이터 파이프라인 완료")
        print("="*80)

    def run_full_pipeline(self):
        """
        전체 데이터 파이프라인 실행

        실행 순서:
        1. 참조 데이터 수집 (업종코드, 법정동코드)
        2. 기업 데이터 파이프라인
        3. 학교 데이터 파이프라인
        """
        print("\n" + "="*80)
        print("전체 데이터 수집 및 전처리 파이프라인 시작")
        print("="*80)

        # Step 0: 참조 데이터 수집 (선행 필수)
        self.run_reference_data_collection()

        # Step 1: 기업 데이터 파이프라인
        self.run_company_pipeline()

        # Step 2: 학교 데이터 파이프라인
        self.run_school_pipeline()

        print("\n" + "="*80)
        print("전체 데이터 파이프라인 완료!")
        print("="*80)


def main():
    """CLI 진입점"""
    import argparse

    parser = argparse.ArgumentParser(
        description='데이터 수집 및 전처리 파이프라인',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
실행 모드:
  reference  - 참조 데이터 수집 (업종코드, 법정동코드)
  company    - 기업 데이터 파이프라인
  school     - 학교 데이터 파이프라인
  full       - 전체 파이프라인 (기본값)

예시:
  python pipeline.py --mode reference  # 참조 데이터만 수집
  python pipeline.py --mode company    # 기업 데이터만 처리
  python pipeline.py                   # 전체 파이프라인 실행
        """
    )
    parser.add_argument(
        '--mode',
        choices=['reference', 'company', 'school', 'full'],
        default='full',
        help='실행 모드'
    )

    args = parser.parse_args()
    pipeline = DataPipeline()

    if args.mode == 'reference':
        pipeline.run_reference_data_collection()
    elif args.mode == 'company':
        pipeline.run_company_pipeline()
    elif args.mode == 'school':
        pipeline.run_school_pipeline()
    else:
        pipeline.run_full_pipeline()


if __name__ == "__main__":
    main()
