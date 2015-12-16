# encoding: utf-8
from __future__ import absolute_import, print_function, unicode_literals
from collections import defaultdict

import logging
import traceback
import os
from datetime import timedelta
from optparse import make_option

from django import db
from django.core.management.base import LabelCommand
from django.db import reset_queries

from haystack import connections as haystack_connections
from haystack.exceptions import HaystackError
from haystack.query import SearchQuerySet
from haystack.utils.app_loading import get_models, load_apps

try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text

try:
    from django.utils.encoding import smart_bytes
except ImportError:
    from django.utils.encoding import smart_str as smart_bytes

try:
    from django.utils.timezone import now
except ImportError:
    from datetime import datetime
    now = datetime.now

from timeit import default_timer

DEFAULT_BATCH_SIZE = None
DEFAULT_AGE = None
APP = 'app'
MODEL = 'model'

def worker(bits):
    # We need to reset the connections, otherwise the different processes
    # will try to share the connection, which causes things to blow up.
    from django.db import connections

    for alias, info in connections.databases.items():
        # We need to also tread lightly with SQLite, because blindly wiping
        # out connections (via ``... = {}``) destroys in-memory DBs.
        if 'sqlite3' not in info['ENGINE']:
            try:
                db.close_connection()
                if isinstance(connections._connections, dict):
                    del(connections._connections[alias])
                else:
                    delattr(connections._connections, alias)
            except KeyError:
                pass

    if bits[0] == 'do_update':
        func, model, start, end, total, using, start_date, end_date, verbosity = bits
    elif bits[0] == 'do_remove':
        func, model, pks_seen, start, upper_bound, using, verbosity = bits
    else:
        return

    unified_index = haystack_connections[using].get_unified_index()
    index = unified_index.get_index(model)
    backend = haystack_connections[using].get_backend()

    if func == 'do_update':
        qs = index.build_queryset(start_date=start_date, end_date=end_date)
        do_update(backend, index, qs, start, end, total, verbosity=verbosity)
    elif bits[0] == 'do_remove':
        do_remove(backend, index, model, pks_seen, start, upper_bound, verbosity=verbosity)

def haystack_message(message):
    return '{{ Haystack }} {}'.format(message)

def do_update(backend, index, qs, start, end, total, verbosity=1):
    # Get a clone of the QuerySet so that the cache doesn't bloat up
    # in memory. Useful when reindexing large amounts of data.
    small_cache_qs = qs.all()
    current_qs = small_cache_qs[start:end]

    index.pre_process_data(current_qs)

    if verbosity >= 2:
        base_text = ' > Indexing {1:>{0}} - {2:>{0}} (of {3:>{0}})'.format(len(str(total)), start + 1, end, total)
        if hasattr(os, 'getppid') and os.getpid() == os.getppid():
            print(haystack_message(base_text))
        else:
            print(haystack_message('{} [PID {}]'.format(base_text, os.getpid())))

    try:
        backend.update(index, current_qs)
    except Exception as e:
        print()
        print(haystack_message('!! { backend.update } threw:'))
        print('-----')
        print(e)
        print('-----')
        traceback.print_stack()
        print()
        raise HaystackError(haystack_message('!! { backend.update } issue: {}'.format(e.message)))

    # Clear out the DB connections queries because it bloats up RAM.
    reset_queries()


def do_remove(backend, index, model, pks_seen, start, upper_bound, verbosity=1):
    # Fetch a list of results.
    # Can't do pk range, because id's are strings (thanks comments
    # & UUIDs!).
    stuff_in_the_index = SearchQuerySet(using=backend.connection_alias).models(model)[start:upper_bound]

    # Iterate over those results.
    for result in stuff_in_the_index:
        # Be careful not to hit the DB.
        if not smart_bytes(result.pk) in pks_seen:
            # The id is NOT in the small_cache_qs, issue a delete.
            if verbosity >= 2:
                print("  removing %s." % result.pk)

            backend.remove(".".join([result.app_label, result.model_name, str(result.pk)]))


def format_timedelta(td_object):
    """
    Turns a timedelta object into a string formatted such as:
        "1 day, 3 hours, 38 minutes, 20 seconds"
    (Adapted from http://stackoverflow.com/a/13756038)
    """
    seconds = int(td_object.total_seconds())

    if not seconds:
        return 'less than a second'

    periods = [
            ('year',        60*60*24*365),
            ('month',       60*60*24*30),
            ('day',         60*60*24),
            ('hour',        60*60),
            ('minute',      60),
            ('second',      1)
    ]

    strings = []

    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            if period_value == 1:
                strings.append("%s %s" % (period_value, period_name))
            else:
                strings.append("%s %ss" % (period_value, period_name))

    return ', '.join(strings)


class Command(LabelCommand):
    help = "Freshens the index for the given app(s)."
    base_options = (
        make_option('-a', '--age', action='store', dest='age',
            default=DEFAULT_AGE, type='int',
            help='Number of hours back to consider objects new.'
        ),
        make_option('-s', '--start', action='store', dest='start_date',
            default=None, type='string',
            help='The start date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-e', '--end', action='store', dest='end_date',
            default=None, type='string',
            help='The end date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-b', '--batch-size', action='store', dest='batchsize',
            default=None, type='int',
            help='Number of items to index at once.'
        ),
        make_option('-r', '--remove', action='store_true', dest='remove',
            default=False, help='Remove objects from the index that are no longer present in the database.'
        ),
        make_option("-u", "--using", action="append", dest="using",
            default=[],
            help='Update only the named backend (can be used multiple times). '
                 'By default all backends will be updated.'
        ),
        make_option('-k', '--workers', action='store', dest='workers',
            default=0, type='int',
            help='Allows for the use multiple workers to parallelize indexing. Requires multiprocessing.'
        ),
    )
    option_list = LabelCommand.option_list + base_options

    def handle(self, *items, **options):
        self.verbosity = int(options.get('verbosity', 1))
        self.batchsize = options.get('batchsize', DEFAULT_BATCH_SIZE)
        self.start_date = None
        self.end_date = None
        self.remove = options.get('remove', False)
        self.workers = int(options.get('workers', 0))

        self.backends = options.get('using')
        if not self.backends:
            self.backends = haystack_connections.connections_info.keys()
        self.logger = logging.getLogger('haystack')

        age = options.get('age', DEFAULT_AGE)
        start_date = options.get('start_date')
        end_date = options.get('end_date')

        if age is not None:
            self.start_date = now() - timedelta(hours=int(age))

        if start_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.start_date = dateutil_parse(start_date)
            except ValueError:
                pass

        if end_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.end_date = dateutil_parse(end_date)
            except ValueError:
                pass

        if not items:
            items = load_apps()

        self.timers = defaultdict(dict)

        return super(Command, self).handle(*items, **options)

    def handle_label(self, label, **options):
        for using in self.backends:
            try:
                self.update_backend(label, using)
            except:
                logging.exception("Error updating %s using %s ", label, using)
                raise


    def log_info(self, message, header=None, footer=None):
        if self.verbosity < 1:
            return

        if header:
            self.logger.info(haystack_message('{}'.format(len(message) * header)))
        self.logger.info(haystack_message('{}'.format(message)))
        if footer:
            self.logger.info(haystack_message('{}'.format(len(message) * footer)))

    def log_error(self, message):
        self.logger.error(haystack_message('ERROR: {}'.format(message)))

    def log_debug(self, message):
        self.logger.debug(haystack_message('(DEBUG) {}'.format(message)))

    def log_start_header(self, model_set_name):
        self.log_start('-- Updating all {} --'.format(model_set_name), timer_name='default', header='=', footer='-')

    def log_start(self, message, timer_name='default', header=None, footer=None):
        if self.verbosity < 1:
            return

        self.timers[timer_name]['start'] = default_timer()

        self.log_info(message, header=header, footer=footer)

    def log_info_and_print(self, message):
        print(haystack_message(message))

        self.log_info(message)

    def log_end(self, message, timer_name='default'):
        if self.verbosity < 1:
            return

        elapsed = timedelta(seconds=(default_timer() - self.timers[timer_name]['start']))

        self.log_info_and_print('{} It took {} ({:.2f}s) total.'.format(
            message,
            format_timedelta(elapsed),
            elapsed.total_seconds()
        ))

    def update_backend(self, label, using):
        from haystack.exceptions import NotHandled

        backend = haystack_connections[using].get_backend()
        unified_index = haystack_connections[using].get_unified_index()

        if self.workers > 0:
            import multiprocessing

        for model in get_models(label):
            model_set_name = model._meta.verbose_name_plural
            try:
                index = unified_index.get_index(model)
            except NotHandled:
                if self.verbosity > 2:
                    print("Skipping '%s' - no index." % model)
                continue

            self.log_start_header(model._meta.verbose_name_plural)

            if self.workers > 0:
                # workers resetting connections leads to references to models / connections getting
                # stale and having their connection disconnected from under them. Resetting before
                # the loop continues and it accesses the ORM makes it better.
                db.close_connection()

            qs = index.build_queryset(using=using, start_date=self.start_date,
                                      end_date=self.end_date)

            total = qs.count()

            if self.verbosity >= 1:
                self.log_info_and_print('> Indexing {} {}...'.format(total, force_text(model_set_name)))

            batch_size = self.batchsize or backend.batch_size

            if self.workers > 0:
                ghetto_queue = []

            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)

                if self.workers == 0:
                    do_update(backend, index, qs, start, end, total, self.verbosity)
                else:
                    ghetto_queue.append(('do_update', model, start, end, total, using, self.start_date, self.end_date, self.verbosity))

            if self.workers > 0:
                pool = multiprocessing.Pool(self.workers)
                pool.map(worker, ghetto_queue)
                pool.terminate()

            self.log_end('< Finished updating backend for {}.'.format(model_set_name))

            if self.remove:
                # For some reason, if we don't close them all out, we find out that the connection
                # has gone away. I assume this is because the remove is happening in the root
                # process and that the connections are messed up.  This only happens when using 
                # the -k or --workers flag.
                db.close_connection()
                if self.start_date or self.end_date or total <= 0:
                    # They're using a reduced set, which may not incorporate
                    # all pks. Rebuild the list with everything.
                    qs = index.index_queryset().values_list('pk', flat=True)
                    pks_seen = set(smart_bytes(pk) for pk in qs)

                    total = len(pks_seen)
                else:
                    pks_seen = set(smart_bytes(pk) for pk in qs.values_list('pk', flat=True))

                if self.workers > 0:
                    ghetto_queue = []

                for start in range(0, total, batch_size):
                    upper_bound = start + batch_size

                    if self.workers == 0:
                        do_remove(backend, index, model, pks_seen, start, upper_bound)
                    else:
                        ghetto_queue.append(('do_remove', model, pks_seen, start, upper_bound, using, self.verbosity))

                if self.workers > 0:
                    pool = multiprocessing.Pool(self.workers)
                    pool.map(worker, ghetto_queue)
                    pool.terminate()
