"""
카카오 로컬 API 클라이언트

주소 → 좌표 변환, 좌표 → 법정동코드 변환
"""
import requests
import urllib3
from typing import Optional, Tuple
from config.settings import APIConfig, ProcessConfig

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KakaoAPIClient:
    """카카오 로컬 API 클라이언트"""

    def __init__(self, api_key: str = None):
        """
        Args:
            api_key: 카카오 REST API 키
        """
        self.api_key = api_key or APIConfig.KAKAO_API_KEY
        self.headers = {"Authorization": f"KakaoAK {self.api_key}"}

    def get_coordinates_from_address(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        주소를 좌표로 변환

        Args:
            address: 검색할 주소

        Returns:
            (longitude, latitude) 튜플 또는 (None, None)
        """
        if not address or not address.strip():
            return None, None

        url = f"{APIConfig.KAKAO_BASE_URL}/search/address.json"
        params = {"query": address}

        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=ProcessConfig.API_TIMEOUT,
                verify=False
            )
            response.raise_for_status()
            data = response.json()

            if data.get('documents') and len(data['documents']) > 0:
                result = data['documents'][0]
                longitude = float(result['x'])
                latitude = float(result['y'])
                return longitude, latitude

        except Exception:
            pass

        return None, None

    def get_legal_dong_from_coord(self, lon: float, lat: float) -> Optional[dict]:
        """
        좌표를 법정동 코드로 변환

        Args:
            lon: 경도
            lat: 위도

        Returns:
            {
                'ctp_cd': 시도코드,
                'ctp_nm': 시도명,
                'sig_cd': 시군구코드,
                'sig_nm': 시군구명,
                'emd_cd': 읍면동코드,
                'emd_nm': 읍면동명
            } 또는 None
        """
        url = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
        params = {
            "x": lon,
            "y": lat,
            "input_coord": "WGS84"
        }

        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=ProcessConfig.API_TIMEOUT,
                verify=False
            )

            if response.status_code != 200:
                return None

            data = response.json()

            if not data.get('documents'):
                return None

            # B (법정동) 타입 찾기
            legal_dong = None
            for doc in data['documents']:
                if doc.get('region_type') == 'B':
                    legal_dong = doc
                    break

            if not legal_dong:
                return None

            code = legal_dong.get('code', '')

            # 10자리 법정동 코드를 시도/시군구/읍면동으로 분리
            ctp_cd = code[:2] + '00000000' if len(code) >= 2 else None
            sig_cd = code[:5] + '00000' if len(code) >= 5 else None
            emd_cd = code if len(code) == 10 else None

            return {
                'ctp_cd': ctp_cd,
                'ctp_nm': legal_dong.get('region_1depth_name', ''),
                'sig_cd': sig_cd,
                'sig_nm': f"{legal_dong.get('region_1depth_name', '')} {legal_dong.get('region_2depth_name', '')}".strip(),
                'emd_cd': emd_cd,
                'emd_nm': f"{legal_dong.get('region_1depth_name', '')} {legal_dong.get('region_2depth_name', '')} {legal_dong.get('region_3depth_name', '')}".strip()
            }

        except Exception:
            return None
