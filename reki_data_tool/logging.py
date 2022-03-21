

def get_logger():
    try:
        import loguru
        return loguru.logger
    except ImportError:
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s'
        )
        logger = logging.getLogger()
        return logger
