"""
업종코드 정보 추가 (API 사용)

한국무역보험공사 업종코드 API를 통해 업종 정보를 가져와서 기업 데이터에 매칭합니다.
"""
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import time
from config.settings import PathConfig, OutputConfig, ProcessConfig
from core.utils import remove_address_duplicates, ReferenceDataManager

def load_industry_classification():
    """
    업종코드 CSV에서 데이터 로드

    Returns:
        DataFrame: 전체 업종코드 데이터 (depth별 구조)
    """
    print("=" * 70)
    print("[1/2] 업종코드 CSV에서 데이터 로드")
    print("=" * 70)

    # final 폴더에서 최신 업종코드 CSV 찾기
    csv_files = list(PathConfig.FINAL_DATA_DIR.glob("업종코드_*.csv"))
    if not csv_files:
        raise FileNotFoundError("업종코드 CSV 파일을 찾을 수 없습니다. collect_industry_codes.py를 먼저 실행하세요.")

    # 날짜별로 정렬 (최신 파일 선택)
    csv_files.sort(key=lambda x: x.stem.split('_')[1] if '_' in x.stem else '', reverse=True)
    latest_file = csv_files[0]

    print(f"[INFO] 로드할 파일: {latest_file.name}")

    # CSV 로드
    df = pd.read_csv(latest_file, encoding=ProcessConfig.ENCODING_DEFAULT)

    print(f"[OK] 업종코드 {len(df)}개 로드 완료")
    print(f"[INFO] 컬럼: {', '.join(df.columns.tolist())}")
    print()

    return df


def add_industry_classification(df, industry_df):
    """
    데이터프레임에 업종코드 정보 추가 (depth별 구조 기반)

    Args:
        df: 기업 데이터프레임 (induty_code 컬럼 필수)
        industry_df: 업종코드 DataFrame (depth별 구조)

    Returns:
        업종 정보가 추가된 데이터프레임
    """
    print("=" * 70)
    print("[2/2] 기업 데이터에 업종코드 정보 추가")
    print("=" * 70)

    # 업종코드 딕셔너리 생성 (빠른 조회)
    # Key: 업종코드 (depth2/3/4/5 중 하나), Value: 전체 depth 정보
    industry_lookup = {}

    for _, row in industry_df.iterrows():
        # depth2~5까지 모두 체크하여 딕셔너리에 추가
        for depth in [2, 3, 4, 5]:
            code_col = f'업종코드_depth{depth}'
            if code_col in industry_df.columns:
                # float을 int로 변환 후 문자열로 (.0 제거)
                code_val = row[code_col]
                if pd.notna(code_val):
                    code = str(int(float(code_val))).strip()
                else:
                    code = ''
                if code and code != '':
                    industry_lookup[code] = {
                        'corp_depth1_cd': str(row['업종코드_depth1']).strip() if pd.notna(row['업종코드_depth1']) else '',
                        'corp_depth1_nm': str(row['업종명_depth1']).strip() if pd.notna(row['업종명_depth1']) else '',
                        'corp_depth2_cd': str(int(float(row['업종코드_depth2']))) if pd.notna(row['업종코드_depth2']) else '',
                        'corp_depth2_nm': str(row['업종명_depth2']).strip() if pd.notna(row['업종명_depth2']) else '',
                        'corp_depth3_cd': str(int(float(row['업종코드_depth3']))) if pd.notna(row['업종코드_depth3']) else '',
                        'corp_depth3_nm': str(row['업종명_depth3']).strip() if pd.notna(row['업종명_depth3']) else '',
                        'corp_depth4_cd': str(int(float(row['업종코드_depth4']))) if pd.notna(row['업종코드_depth4']) else '',
                        'corp_depth4_nm': str(row['업종명_depth4']).strip() if pd.notna(row['업종명_depth4']) else '',
                        'corp_depth5_cd': str(int(float(row['업종코드_depth5']))) if pd.notna(row['업종코드_depth5']) else '',
                        'corp_depth5_nm': str(row['업종명_depth5']).strip() if pd.notna(row['업종명_depth5']) else '',
                    }

    # 새 컬럼 초기화
    df['corp_depth1_cd'] = None  # 대분류 알파벳 코드 (A~U)
    df['corp_depth1_nm'] = None  # 대분류 명칭
    df['corp_depth2_cd'] = None  # 중분류 코드 (2자리)
    df['corp_depth2_nm'] = None  # 중분류 명칭
    df['corp_depth3_cd'] = None  # 소분류 코드 (3자리)
    df['corp_depth3_nm'] = None  # 소분류 명칭
    df['corp_depth4_cd'] = None  # 세분류 코드 (4자리)
    df['corp_depth4_nm'] = None  # 세분류 명칭
    df['corp_depth5_cd'] = None  # 세세분류 코드 (5자리)
    df['corp_depth5_nm'] = None  # 세세분류 명칭

    total = len(df)
    matched_count = 0

    for idx, row in df.iterrows():
        if (idx + 1) % 500 == 0 or idx == 0 or (idx + 1) == total:
            print(f"진행 중: {idx + 1}/{total} ({(idx + 1) / total * 100:.1f}%) - 매칭: {matched_count}개")

        induty_code = row['induty_code']

        # 업종 코드가 있는 경우
        if pd.notna(induty_code):
            code = str(induty_code).strip()

            # 업종코드 딕셔너리에서 직접 조회
            if code in industry_lookup:
                info = industry_lookup[code]

                # 모든 depth 정보 한번에 할당
                df.at[idx, 'corp_depth1_cd'] = info['corp_depth1_cd']
                df.at[idx, 'corp_depth1_nm'] = info['corp_depth1_nm']
                df.at[idx, 'corp_depth2_cd'] = info['corp_depth2_cd']
                df.at[idx, 'corp_depth2_nm'] = info['corp_depth2_nm']
                df.at[idx, 'corp_depth3_cd'] = info['corp_depth3_cd']
                df.at[idx, 'corp_depth3_nm'] = info['corp_depth3_nm']
                df.at[idx, 'corp_depth4_cd'] = info['corp_depth4_cd']
                df.at[idx, 'corp_depth4_nm'] = info['corp_depth4_nm']
                df.at[idx, 'corp_depth5_cd'] = info['corp_depth5_cd']
                df.at[idx, 'corp_depth5_nm'] = info['corp_depth5_nm']

                matched_count += 1

    print(f"\n[OK] 완료!")
    print(f"업종코드 매칭: {matched_count}개 ({matched_count / total * 100:.1f}%)")
    print()

    return df

def main():
    """
    메인 실행 함수
    """
    start_time = time.time()

    try:
        # 1. 업종코드 CSV에서 데이터 로드
        industry_df = load_industry_classification()

        # 2. 기업 데이터 로드
        input_path = PathConfig.INTERMEDIATE_DATA_DIR / OutputConfig.INTERMEDIATE_WITH_COORDINATES

        if not input_path.exists():
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_path}")

        df = pd.read_csv(input_path, encoding=ProcessConfig.ENCODING_DEFAULT)
        print(f"기업 데이터 로드: {len(df)}개")
        print()

        # 3. 업종코드 정보 추가
        df_with_industry = add_industry_classification(df, industry_df)

        # 4. 최종 컬럼 선택 및 이름 변경
        print("=" * 70)
        print("최종 CSV 생성")
        print("=" * 70)
        print()

        final_df = df_with_industry[[
            'corp_code',
            'corp_name',
            'ctp_cd',
            'ctp_nm',
            'sig_cd',
            'sig_nm',
            'emd_cd',
            'emd_nm',
            'adres',
            'longitude',
            'latitude',
            'corp_depth1_cd',
            'corp_depth1_nm',
            'corp_depth2_cd',
            'corp_depth2_nm',
            'corp_depth3_cd',
            'corp_depth3_nm',
            'corp_depth4_cd',
            'corp_depth4_nm',
            'corp_depth5_cd',
            'corp_depth5_nm'
        ]].copy()

        # 컬럼명 변경
        final_df.columns = [
            'corp_cd',           # 기업 코드
            'corp_nm',           # 기업명
            'ctp_cd',            # 시도 코드
            'ctp_nm',            # 시도명
            'sig_cd',            # 시군구 코드
            'sig_nm',            # 시군구명
            'emd_cd',            # 읍면동 코드
            'emd_nm',            # 읍면동명
            'all_addr_nm',       # 전체 주소
            'lon',               # 경도
            'lat',               # 위도
            'corp_depth1_cd',    # 대분류 코드
            'corp_depth1',       # 대분류명
            'corp_depth2_cd',    # 중분류 코드
            'corp_depth2',       # 중분류명
            'corp_depth3_cd',    # 소분류 코드
            'corp_depth3',       # 소분류명
            'corp_depth4_cd',    # 세분류 코드
            'corp_depth4',       # 세분류명
            'corp_depth5_cd',    # 세세분류 코드
            'corp_depth5'        # 세세분류명
        ]

        # 5. 주소 중복 제거 적용 (기업 데이터 규칙 A)
        print("주소 중복 제거 적용 중...")
        cleaned_count = 0
        for idx, row in final_df.iterrows():
            ctp_nm_clean, sig_nm_clean, emd_nm_clean = remove_address_duplicates(
                row['ctp_nm'], row['sig_nm'], row['emd_nm']
            )

            # 변경 여부 확인
            if (ctp_nm_clean != row['ctp_nm'] or
                sig_nm_clean != row['sig_nm'] or
                emd_nm_clean != row['emd_nm']):
                cleaned_count += 1

            final_df.at[idx, 'ctp_nm'] = ctp_nm_clean
            final_df.at[idx, 'sig_nm'] = sig_nm_clean
            final_df.at[idx, 'emd_nm'] = emd_nm_clean

        print(f"[OK] 주소 중복 제거 완료: {cleaned_count}개 ({cleaned_count / len(final_df) * 100:.1f}%)")
        print()

        # 6. 법정동코드 컬럼을 정수형 문자열로 변환
        code_columns = ['ctp_cd', 'sig_cd', 'emd_cd']
        for col in code_columns:
            if col in final_df.columns:
                final_df[col] = final_df[col].apply(lambda x: str(int(x)) if pd.notna(x) and x != '' else '')

        # 7. 산업분류 코드 컬럼을 정수형 문자열로 변환 (corp_depth1_cd는 문자 포함되어 제외)
        industry_code_columns = ['corp_cd', 'corp_depth2_cd', 'corp_depth3_cd',
                                'corp_depth4_cd', 'corp_depth5_cd']
        for col in industry_code_columns:
            if col in final_df.columns:
                final_df[col] = final_df[col].apply(lambda x: str(int(x)) if pd.notna(x) and x != '' else '')

        # 8. 위경도 소수점 10자리로 제한
        for col in ['lat', 'lon']:
            if col in final_df.columns:
                final_df[col] = final_df[col].apply(lambda x: round(x, 10) if pd.notna(x) else x)

        # 9. 저장 (날짜별로 final 폴더에 저장)
        ref_manager = ReferenceDataManager("기업위치")
        ref_manager.save_to_csv(final_df)
        output_path = ref_manager.get_latest_csv_file()

        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print("=" * 70)
        print("작업 완료!")
        print("=" * 70)
        print(f"저장 파일: {output_path}")
        print(f"총 기업 수: {len(final_df):,}개")
        print(f"컬럼 수: {len(final_df.columns)}개")
        print(f"소요 시간: {minutes}분 {seconds}초")
        print("=" * 70)
        print()

        # 데이터 품질 통계
        print("=" * 70)
        print("데이터 품질 통계")
        print("=" * 70)
        print(f"총 기업 수: {len(final_df):,}개")
        print()

        # 행정구역 매칭률
        ctp_matched = final_df['ctp_cd'].notna().sum()
        print(f"행정구역 매칭률: {ctp_matched:,}개 ({ctp_matched / len(final_df) * 100:.1f}%)")

        # 좌표 변환 성공률
        coords_matched = final_df['lon'].notna().sum()
        print(f"좌표 변환 성공률: {coords_matched:,}개 ({coords_matched / len(final_df) * 100:.1f}%)")

        # 산업분류 완성도
        industry_matched = final_df['corp_depth1_cd'].notna().sum()
        print(f"산업분류 매칭률: {industry_matched:,}개 ({industry_matched / len(final_df) * 100:.1f}%)")
        print("=" * 70)
        print()

        # 샘플 데이터 출력
        print("샘플 데이터 (첫 3개):")
        sample_cols = [
            'corp_cd', 'corp_nm', 'ctp_nm', 'sig_nm', 'emd_nm',
            'corp_depth1', 'corp_depth2', 'corp_depth3'
        ]
        print(final_df[sample_cols].head(3).to_string())
        print()

    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        raise

if __name__ == "__main__":
    main()
