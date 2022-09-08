from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL, basicConfig, Formatter, Logger, StreamHandler, basicConfig, getLogger
stream_handler = StreamHandler()
stream_handler.setLevel(DEBUG)
stream_handler.setFormatter(
    Formatter("[%(asctime)s]:%(levelname)s:%(message)s"))
basicConfig(level=DEBUG, handlers=[stream_handler])
