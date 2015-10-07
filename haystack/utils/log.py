from __future__ import absolute_import
from __future__ import unicode_literals
from collections import defaultdict

import logging

from django.conf import settings


def getLogger(name):
    real_logger = logging.getLogger(name)
    return LoggingFacade(real_logger)


class LoggingFacade(object):
    def __init__(self, real_logger):
        self.real_logger = real_logger

    def noop(self, *args, **kwargs):
        pass

    def __getattr__(self, attr):
        if getattr(settings, 'HAYSTACK_LOGGING', True):
            return getattr(self.real_logger, attr)
        return self.noop

def print_regular(message):
    print('[Haystack] %s' % message)

def print_timing(method_name, elapsed):
    print_regular('%s [%2.3fs]' % (method_name, elapsed))

class Counter():
    def __init__(self):
        self.registries = defaultdict(lambda: 0)

    def add(self, registry_name, amount):
        self.registries[registry_name] += amount

    def total(self, registry_name):
        return self.registries[registry_name]

_counter = Counter()

def get_counter():
    global _counter
    return _counter
