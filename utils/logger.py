import logging


class Logger:
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @staticmethod
    def get_logger(name=__name__, level=logging.DEBUG):
        logging.basicConfig(level=level, filename='mexc.log', format=Logger.log_format)
        return logging.getLogger(name)
