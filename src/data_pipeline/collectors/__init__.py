"""
Data Collectors 모듈

각 소스에서 데이터를 수집하여 CSV로 저장합니다.

Collectors:
- collect_dart_data: DART API에서 상장기업 정보 수집
- collect_high_schools: SafeMap API에서 고등학교 정보 수집
- collect_universities: SafeMap API에서 대학교 정보 수집
- collect_industry_codes: 한국무역보험공사 API에서 업종코드 수집
- collect_legal_dong_codes: 공공데이터포털 API에서 법정동코드 수집
"""
__all__ = [
    'collect_dart_data',
    'collect_high_schools',
    'collect_universities',
    'collect_industry_codes',
    'collect_legal_dong_codes',
]
