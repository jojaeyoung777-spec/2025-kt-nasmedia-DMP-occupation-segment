"""
Data Processors 모듈

수집된 데이터를 전처리하고 보강합니다.

Processors:
- add_coordinates: 기업 데이터에 카카오 API로 좌표 및 법정동 정보 추가
- add_industry_classification: 기업 데이터에 업종코드 정보 매칭 및 최종 CSV 생성
- match_school_legal_dong_codes: 학교 데이터에 역지오코딩으로 법정동 정보 추가
- enrich_with_kakao_api: 기업 데이터 보강 (좌표 결측치 + 법정동 코드 보정 + 주소 중복 제거)
"""
__all__ = [
    'add_coordinates',
    'add_industry_classification',
    'match_school_legal_dong_codes',
    'enrich_with_kakao_api',
]
