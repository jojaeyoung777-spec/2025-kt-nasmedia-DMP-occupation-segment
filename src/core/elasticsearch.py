"""
Elasticsearch 클라이언트 팩토리

Elasticsearch 연결 및 클라이언트 생성을 담당합니다.
"""
from elasticsearch import Elasticsearch
from config.settings import ElasticsearchConfig


def create_es_client() -> Elasticsearch:
    """
    Elasticsearch 클라이언트 생성

    Returns:
        Elasticsearch: 설정된 ES 클라이언트 인스턴스
    """
    es_config = ElasticsearchConfig.ES_CONFIG

    # URL 형식으로 변경
    scheme = 'https' if es_config.get('ca_certs') else 'http'
    es_url = f"{scheme}://{es_config['host']}:{es_config['port']}"

    # 최신 Elasticsearch 클라이언트 API 사용
    es_params = {
        'hosts': [es_url]
    }

    if es_config.get('user') and es_config.get('password'):
        es_params['basic_auth'] = (es_config['user'], es_config['password'])

    if es_config.get('ca_certs'):
        es_params['ca_certs'] = es_config['ca_certs']
        es_params['verify_certs'] = False

    return Elasticsearch(**es_params)


def get_es_url() -> str:
    """
    Elasticsearch URL 생성

    Returns:
        str: ES 연결 URL (예: https://10.10.20.61:19200)
    """
    es_config = ElasticsearchConfig.ES_CONFIG
    scheme = 'https' if es_config.get('ca_certs') else 'http'
    return f"{scheme}://{es_config['host']}:{es_config['port']}"
