"""
API Clients 모듈

Note: 여기서는 실시간 API 호출이 필요한 클라이언트만 관리합니다.
"""
from .kakao_api import KakaoAPIClient

__all__ = [
    'KakaoAPIClient',
]
