"""
로깅 설정

전체 파이프라인의 로깅을 통합 관리합니다.
"""
import logging
from config.settings import LogConfig


def setup_logging():
    """
    로깅 설정 초기화
    """
    logging.basicConfig(
        format=LogConfig.LOG_FORMAT,
        datefmt=LogConfig.LOG_DATE_FORMAT,
        level=getattr(logging, LogConfig.LOG_LEVEL)
    )


def get_logger(name: str) -> logging.Logger:
    """
    이름으로 logger 가져오기

    Args:
        name: 로거 이름 (보통 모듈명 사용)

    Returns:
        logging.Logger: 설정된 로거 인스턴스
    """
    return logging.getLogger(name)
