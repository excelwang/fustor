# src/fustor_agent/logging_setup.py

import logging
import logging.config
import os

def setup_logging(log_directory: str, level: int = logging.INFO, log_file_name: str = 'fustor_agent.log', console_output: bool = True):
    """
    设置应用程序的声明式日志配置。
    使用 dictConfig 来提供一个集中、可维护的日志配置方案。
    """
    log_file_path = os.path.join(log_directory, log_file_name)

    # 确保日志目录存在
    os.makedirs(log_directory, exist_ok=True)

    # 在启动时清空日志文件
    if os.path.exists(log_file_path):
        try:
            # 使用 'w' 模式打开文件会立即截断它
            with open(log_file_path, 'w'):
                pass
        except OSError as e:
            # 如果因权限等问题无法清空，打印警告但不要让程序崩溃
            print(f"Warning: Could not clear log file {log_file_path}. Error: {e}")

    # 根据参数动态确定 handlers
    base_handlers = ['file']
    if console_output:
        base_handlers.append('console')

    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
        },
        'handlers': {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'standard',
                'filename': log_file_path,
                'maxBytes': 10485760, # 10 MB
                'backupCount': 5,
                'encoding': 'utf8'
            },
            'console': { 
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            }
        },
        'loggers': {
            'fustor_agent': { 
                'handlers': base_handlers,
                'level': level,
                'propagate': False
            },
            'uvicorn': { 
                'handlers': base_handlers,
                'level': logging.INFO,
                'propagate': False
            },
            'uvicorn.error': { 
                'handlers': base_handlers,
                'level': logging.INFO,
                'propagate': False
            },
            'uvicorn.access': { 
                'handlers': base_handlers,
                'level': logging.INFO,
                'propagate': False
            }
        },
        'root': { 
            'handlers': base_handlers,
            'level': logging.ERROR, # Keep root at ERROR to avoid noise
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)

    # --- START: Silence Third-Party Loggers ---
    third_party_loggers = [
        "httpx",
        "httpcore",
    ]
    for logger_name in third_party_loggers:
        # Only set to ERROR if the main level is not DEBUG
        if level > logging.DEBUG:
            logging.getLogger(logger_name).setLevel(logging.WARNING)
    # --- END: Silence Third-Party Loggers ---

    logging.getLogger("fustor_agent")
    logging.getLogger("uvicorn")
    logging.getLogger("uvicorn.error")
    logging.getLogger("uvicorn.access")
