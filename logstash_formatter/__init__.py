'''
This library is provided to allow standard python
logging to output log data as JSON formatted strings
ready to be shipped out to logstash.
'''
import logging
import socket
import datetime
import traceback as tb
import json

def _default_json_default(obj):
    """
    Coerce everything to strings.
    All objects representing time get output as ISO8601.
    """
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    else:
        return str(obj)

class LogstashFormatter(logging.Formatter):
    """
    A custom formatter to prepare logs to be
    shipped out to logstash.
    """

    def __init__(self,
                 fmt=None,
                 datefmt=None,
                 json_cls=None,
                 json_default=_default_json_default):
        """
        :param fmt: Config as a JSON string, allowed fields;
               extra: provide extra fields always present in logs
               source_host: override source host name
        :param datefmt: Date format to use (required by logging.Formatter
            interface but not used)
        :param json_cls: JSON encoder to forward to json.dumps
        :param json_default: Default JSON representation for unknown types,
                             by default coerce everything to a string
        """

        if fmt is not None:
            self._fmt = json.loads(fmt)
        else:
            self._fmt = {}
        self.json_default = json_default
        self.json_cls = json_cls
        if 'extra' not in self._fmt:
            self.defaults = {}
        else:
            self.defaults = self._fmt['extra']
        if 'source_host' in self._fmt:
            self.source_host = self._fmt['source_host']
        else:
            try:
                self.source_host = socket.gethostname()
            except Exception:
                self.source_host = ""
        try:
            self.host = socket.gethostbyname(socket.gethostname())
        except Exception:
            self.host = ''

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        fields = record.__dict__.copy()

        if isinstance(record.msg, dict):
            fields.update(record.msg)
            fields.pop('msg')
            msg = ""
        else:
            msg = record.getMessage()

        try:
            msg = msg.format(**fields)
        except (KeyError, IndexError):
            pass

        if fields.get('exc_info'):
            formatted = tb.format_exception(*fields['exc_info'])
            fields['exception'] = formatted

        loglevel = fields.pop('levelname', '')

        worker_guid = fields.pop('name', '')

        unwanted_tags = ['message', 'exc_text', 'exc_info', 'msg', 'lineno', 'filename', 'funcName', 'levelno',
                         'module', 'msecs', 'pathname', 'process', 'processName', 'relativeCreated', 'thread',
                         'threadName']
        for tag in unwanted_tags:
            fields.pop(tag, '')

        logr = self.defaults.copy()

        logr.update({'@message': msg,
                     '@timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     '@source_host': self.source_host,
                     'loglevel': loglevel,
                     'worker_guid': worker_guid,
                     'logging_type': 'redis',
                     '@host': self.host,
                     '@fields': self._build_fields(logr, fields)})

        return json.dumps(logr, default=self.json_default, cls=self.json_cls)

    def _build_fields(self, defaults, fields):
        """Return provided fields including any in defaults

        >>> f = LogstashFormatter()
        # Verify that ``fields`` is used
        >>> f._build_fields({}, {'foo': 'one'}) == \
                {'foo': 'one'}
        True
        # Verify that ``@fields`` in ``defaults`` is used
        >>> f._build_fields({'@fields': {'bar': 'two'}}, {'foo': 'one'}) == \
                {'foo': 'one', 'bar': 'two'}
        True
        # Verify that ``fields`` takes precedence
        >>> f._build_fields({'@fields': {'foo': 'two'}}, {'foo': 'one'}) == \
                {'foo': 'one'}
        True
        """
        return dict(list(defaults.get('@fields', {}).items()) + list(fields.items()))
