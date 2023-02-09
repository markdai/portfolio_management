"""
This module contains decorator for Logging and Testing.

    Original Author: Mark D
    Date created: 08/24/2019
    Date Modified: 09/07/2019
    Python Version: 3.7

Note:
    This module depend on following third-party Python library:
     - none

Examples:
    logger_ref = UseLogging(__name__)
    logger = logger_ref.use_stream_logger()
    logger.info('This is just an example.')

    logger_ref = UseLogging(__name__)
    logger_ref.logging_level = 'DEBUG'
    logger = logger_ref.use_file_logger('temp_solution')
    logger.debug('This is another example.')

"""

from datetime import datetime
import logging


class UseLogging(object):
    """
    The :class: UseLogging will return a logging object with default setting.
    """
    def __init__(self, v_logger_name):
        """
        constructor for :class: UseLogging.
        """
        self._logging_level = logging.INFO
        self._logging_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self._logger = logging.getLogger(v_logger_name)
        self._logger.setLevel(logging.DEBUG)

    @property
    def logging_level(self):
        """
        getter function for :variable: self._logging_level.
        """
        return self._logging_level

    @logging_level.setter
    def logging_level(self, v_logging_level):
        """
        setter function for :variable: self._logging_level.
        """
        if v_logging_level.upper().strip() not in ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']:
            raise IOError("Error: Logging Level is not valid ! Expect CRITICAL/ERROR/WARNING/INFO/DEBUG, got {}: {}".
                          format(str(type(v_logging_level)), str(v_logging_level))
                          )
        else:
            if v_logging_level.upper().strip() == 'DEBUG':
                self._logging_level = logging.DEBUG
            elif v_logging_level.upper().strip() == 'ERROR':
                self._logging_level = logging.ERROR
            elif v_logging_level.upper().strip() == 'WARNING':
                self._logging_level = logging.WARNING
            elif v_logging_level.upper().strip() == 'INFO':
                self._logging_level = logging.INFO
            elif v_logging_level.upper().strip() == 'CRITICAL':
                self._logging_level = logging.CRITICAL

    def use_stream_logger(self):
        """
        The :function: use_stream_logger will create a logging object which send logging output to streams
            such as sys.stdout and sys.stderr.

        Returns:
            :logging.logger: object.

        """
        _stream_handler = logging.StreamHandler()
        _stream_handler.setLevel(self._logging_level)
        _stream_handler.setFormatter(self._logging_format)
        if not len(self._logger.handlers):
            self._logger.addHandler(_stream_handler)
        return self._logger

    def use_file_logger(self, v_log_file_prefix):
        """
        The :function: use_file_logger will create a logging object which send logging output to a file.

        Args:
            v_log_file_prefix (str): Filename prefix for logging output.

        Returns:
            :logging.logger: object.

        """
        _file_handler = logging.FileHandler('logs/'+v_log_file_prefix+'_logging_' +
                                            datetime.now().strftime('%Y%m%d_%H%M%S') + '.log')
        _file_handler.setLevel(self._logging_level)
        _file_handler.setFormatter(self._logging_format)
        if not len(self._logger.handlers):
            self._logger.addHandler(_file_handler)
        return self._logger

    def use_loggers(self, v_log_file_prefix):
        """
        The :function: use_loggers will create a logging object which send logging output to streams and file.

        Args:
            v_log_file_prefix (str): Filename prefix for logging output.

        Returns:
            :logging.logger: object.

        """
        _stream_handler = logging.StreamHandler()
        _stream_handler.setLevel(self._logging_level)
        _stream_handler.setFormatter(self._logging_format)
        self._logger.addHandler(_stream_handler)
        _file_handler = logging.FileHandler('logs/'+v_log_file_prefix+'_logging_' +
                                            datetime.now().strftime('%Y%m%d_%H%M%S') + '.log')
        _file_handler.setLevel(self._logging_level)
        _file_handler.setFormatter(self._logging_format)
        self._logger.addHandler(_file_handler)
        return self._logger
