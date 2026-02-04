"""
Core 모듈 - 공통 유틸리티
"""
from .utils import (
    run_with_fallback,
    save_dataframe_safely,
    check_file_exists_skip,
    ReferenceDataManager,
    clean_address_for_search,
    remove_address_duplicates,
)

# Elasticsearch는 선택적 의존성
try:
    from .elasticsearch import create_es_client, get_es_url
except ImportError:
    create_es_client = None
    get_es_url = None

from .logging import setup_logging, get_logger

__all__ = [
    # utils
    'run_with_fallback',
    'save_dataframe_safely',
    'check_file_exists_skip',
    'ReferenceDataManager',
    'clean_address_for_search',
    'remove_address_duplicates',
    # elasticsearch
    'create_es_client',
    'get_es_url',
    # logging
    'setup_logging',
    'get_logger',
]
