# src/fustor_common/logging_config.py

import logging
import logging.config
import os
import sys

def setup_logging(
    log_directory: str,
    base_logger_name: str,
    level: int = logging.INFO,
    log_file_name: str = 'fustor.log',
    console_output: bool = True
):
    """
    通用日志配置函数。

    Args:
        log_directory (str): 日志文件存放的目录。
        base_logger_name (str): 您的应用程序的基础logger名称（例如，"fustor_agent"或"fustor_fusion"）。
        level (int): 控制台和文件处理程序的最低日志级别。
        log_file_name (str): 日志文件的名称。
        console_output (bool): 是否将日志输出到控制台。
    """
    # 确保日志目录存在
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, log_file_name)

    if isinstance(level, str):
        numeric_level = getattr(logging, level.upper(), logging.INFO)
    else:
        numeric_level = level

    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            },
            'color_console': {
                '()': 'colorlog.ColoredFormatter',
                'format': '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s%(reset)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
                'log_colors': {
                    'DEBUG':    'cyan',
                    'INFO':     'green',
                    'WARNING':  'yellow',
                    'ERROR':    'red',
                    'CRITICAL': 'bold_red',
                }
            },
            'json': {
                '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s'
            }
        },
        'handlers': {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': numeric_level,
                'formatter': 'standard',
                'filename': log_file_path,
                'maxBytes': 10485760, # 10MB
                'backupCount': 5,
                'encoding': 'utf8'
            },
            'console': {
                'class': 'logging.StreamHandler',
                'level': numeric_level,
                'formatter': 'color_console',
                'stream': sys.stdout
            },
            'error_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': logging.ERROR,
                'formatter': 'standard',
                'filename': os.path.join(log_directory, 'fustor_error.log'),
                'maxBytes': 10485760, # 10MB
                'backupCount': 2,
                'encoding': 'utf8'
            }
        },
        'loggers': {
            base_logger_name: {
                'handlers': ['file', 'error_file'],
                'level': numeric_level,
                'propagate': False
            },
            'uvicorn': {
                'handlers': ['file', 'error_file'],
                'level': numeric_level,
                'propagate': False
            },
            'uvicorn.error': {
                'handlers': ['file', 'error_file'],
                'level': numeric_level,
                'propagate': False
            },
            'uvicorn.access': {
                'handlers': ['file', 'error_file'],
                'level': numeric_level, # Use the configured level for access logs
                'propagate': False
            }
        },
        'root': {
            'handlers': ['file', 'error_file'],
            'level': logging.ERROR, # Keep root at ERROR to avoid noise
            'propagate': True, # Allow root to emit to its handlers for unhandled exceptions
        }
    }

    if console_output:
        LOGGING_CONFIG['loggers'][base_logger_name]['handlers'].append('console')
        LOGGING_CONFIG['loggers']['uvicorn']['handlers'].append('console')
        LOGGING_CONFIG['loggers']['uvicorn.error']['handlers'].append('console')
        LOGGING_CONFIG['loggers']['uvicorn.access']['handlers'].append('console')
        LOGGING_CONFIG['root']['handlers'].append('console')

    logging.config.dictConfig(LOGGING_CONFIG)

    # --- START: Silence Third-Party Loggers (more selectively) ---
    # Only silence if the specified level is not DEBUG, to allow debugging when needed
    if numeric_level > logging.DEBUG:
        third_party_loggers = [
            'httpx', 'asyncio', 'watchdog', 'sqlalchemy',
            'alembic', 'requests', 'urllib3', 'multipart' # Add other chatty libraries here
        ]
        for logger_name in third_party_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)
    # --- END: Silence Third-Party Loggers ---

    # Ensure main logger is available for immediate use
    main_logger = logging.getLogger(base_logger_name)
    main_logger.info(f"Logging configured successfully. Level: {logging.getLevelName(numeric_level)}")
    main_logger.debug(f"Log file: {log_file_path}")
