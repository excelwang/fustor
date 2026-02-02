# src/fustor_common/logging_config.py

import logging
import logging.config
import os
import sys

class UvicornAccessFilter(logging.Filter):
    """
    Filter to suppress uvicorn.access logs when system level is INFO,
    but allow them when system level is DEBUG.
    """
    def __init__(self, normal_level: int = logging.INFO):
        super().__init__()
        self.normal_level = normal_level

    def filter(self, record):
        # If it's a uvicorn.access log, only allow it through if system level is DEBUG
        if record.name.startswith('uvicorn.access'):
            return self.normal_level <= logging.DEBUG
        # For other logs, apply normal level filtering
        else:
            return record.levelno >= self.normal_level

def setup_logging(
    log_file_path: str, # Now accepts full path
    base_logger_name: str,
    level: int = logging.INFO,
    console_output: bool = True
):
    """
    通用日志配置函数。

    Args:
        log_file_path (str): 日志文件存放的完整路径。
        base_logger_name (str): 您的应用程序的基础logger名称（例如，"fustor_agent"或"fustor_fusion"）。
        level (int): 控制台和文件处理程序的最低日志级别。
        console_output (bool): 是否将日志输出到控制台。
    """
    # 确保日志文件所在的目录存在
    log_directory = os.path.dirname(log_file_path)
    os.makedirs(log_directory, exist_ok=True)
    
    # Truncate the log file at startup
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'w', encoding='utf8'):
                pass  # Open and immediately close to truncate
        except IOError as e:
            # Log the error, but don't prevent further logging
            logging.getLogger(base_logger_name).error(f"Failed to truncate log file {log_file_path}: {e}")


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
                'level': logging.DEBUG,  # Set to DEBUG so it can catch DEBUG level logs
                'formatter': 'standard',
                'filename': log_file_path,
                'maxBytes': 10485760, # 10MB
                'backupCount': 5,
                'encoding': 'utf8',
                'filters': ['uvicorn_access_filter']  # Add the custom filter
            },
            'console': {
                'class': 'logging.StreamHandler',
                'level': logging.DEBUG,  # Set to DEBUG for the same reason
                'formatter': 'color_console',
                'stream': sys.stdout,
                'filters': ['uvicorn_access_filter']  # Add the custom filter
            }
        },
        'filters': {
            'uvicorn_access_filter': {
                '()': UvicornAccessFilter,
                'normal_level': numeric_level
            }
        },
        'loggers': {
            'fustor': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'fustor_core': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'fustor_agent': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'fustor_source_fs': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'fustor_sender_http': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'fustor_sender_echo': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'uvicorn': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'uvicorn.error': {
                'handlers': ['file'],
                'level': numeric_level,
                'propagate': False
            },
            'uvicorn.access': {
                'handlers': ['file'],
                'level': logging.INFO,
                'propagate': False
            }
        },
        'root': {
            'handlers': ['file'],
            'level': logging.ERROR, # Keep root at ERROR to avoid noise
            'propagate': True, # Allow root to emit to its handlers for unhandled exceptions
        }
    }

    if console_output:
        for logger_name in LOGGING_CONFIG['loggers']:
             LOGGING_CONFIG['loggers'][logger_name]['handlers'].append('console')
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
