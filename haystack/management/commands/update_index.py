# encoding: utf-8
from __future__ import absolute_import, print_function, unicode_literals

import logging
import traceback
import os
from datetime import timedelta
from optparse import make_option

from django import db
from django.core.management.base import LabelCommand
from django.db import reset_queries

from haystack import connections as haystack_connections
from haystack.exceptions import ConnectionError
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


def worker(bits):
    if bits[0] == 'do_update':
        func, model, start, end, total, using, start_date, end_date, verbosity = bits[:9]
    else:
        raise NotImplementedError('Unknown function {}'.format(bits[0]))

    try:
        time_start_batch = default_timer()

        model_set_name = model._meta.verbose_name_plural
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

        model_set_name = model._meta.verbose_name_plural

        backend = haystack_connections[using].get_backend()
        index = haystack_connections[using].get_unified_index().get_index(model)
        query_set = index.build_queryset(start_date=start_date, end_date=end_date)

        do_update(backend, index, query_set, start, end, total, verbosity=verbosity)

        time_end_batch = default_timer()

        elapsed = timedelta(seconds=(time_end_batch - time_start_batch))

        print('[Haystack]   << Finished a {} batch. It took {} ({:.2f}s) total. [{}]'.format(
            model_set_name,
            format_timedelta(elapsed),
            elapsed.total_seconds(),
            '{} - {} ({})'.format(start + 1, end, total)
        ))

    except:
        print()
        print('[Haystack] JOB FAIL')
        traceback.print_stack()
        print()
        raise

def do_update(backend, index, query_set, start, end, total, verbosity=1):
    # Get a clone of the QuerySet so that the cache doesn't bloat up
    # in memory. Useful when reindexing large amounts of data.
    current_qs = query_set.all()[start:end]

    index.pre_process_data(current_qs)

    if verbosity >= 2:
        base_text = '[Haystack]   >> indexing {1:>{0}} - {2:>{0}} (of {3:>{0}})'.format(len(str(total)), start + 1, end, total)
        if hasattr(os, 'getppid') and os.getpid() == os.getppid():
            print(base_text)
        else:
            print('{} [PID {}]'.format(base_text, os.getpid()))

    backend.update(index, current_qs)

    # Clear out the DB connections queries because it bloats up RAM.
    reset_queries()


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
        make_option('-t', '--timeout', action='store', dest='timeout',
            default=0, type='float',
            help='Number of seconds after which a worker process will be considered timed out'
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
        self.timeout = options.get('timeout')
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

        return super(Command, self).handle(*items, **options)

    def log_info(self, message):
        self.logger.info('[Haystack] {}'.format(message))

    def log_error(self, message):
        self.logger.error('[Haystack] ERROR: {}'.format(message))

    def log_debug(self, message):
        self.logger.debug('[Haystack] (DEBUG) {}'.format(message))

    def handle_label(self, label, **options):
        for using in self.backends:
            try:
                self.update_backend(label, using)
            except ConnectionError as ce:
                self.log_error('Encountered a connection error while attempting to update "{}" (using {}) "{}".'.format(label, using, ce.message))
                pass
            except:
                self.log_error('Encountered unknown issue while attempting to update "{}" (using {})'.format(label, using))
                raise

    def log_start(self, model_set_name):
        if self.verbosity >= 1:
            self.time_start_model = default_timer()

            self.log_info('Updating backend for: {}'.format(model_set_name))

    def log_end(self, model_set_name):
        if self.verbosity >= 1:
            self.time_end_model = default_timer()

            elapsed = timedelta(seconds=(self.time_end_model - self.time_start_model))

            self.log_info('Finished updating backend for {}. It took {} ({:.2f}s) total.'.format(
                model_set_name,
                format_timedelta(elapsed),
                elapsed.total_seconds()
            ))

    def update_backend(self, label, using):
        from haystack.exceptions import NotHandled

        backend = haystack_connections[using].get_backend()
        unified_index = haystack_connections[using].get_unified_index()

        for model in get_models(label):
            model_set_name = model._meta.verbose_name_plural

            try:
                index = unified_index.get_index(model)
            except NotHandled:
                if self.verbosity >= 2:
                    self.log_debug('Skipping "{}" - no index.'.format(model))
                continue

            self.log_start(model_set_name)

            query_set = index.build_queryset(using=using, start_date=self.start_date,
                                      end_date=self.end_date)

            total = query_set.count()

            if self.verbosity >= 1:
                self.log_info('Indexing {} {}...'.format(total, force_text(model_set_name)))

            batch_size = self.batchsize or backend.batch_size

            batches = ((start, min(start + batch_size, total)) for start in range(0, total, batch_size))

            if self.workers:
                from pebble import TimeoutError as PebbleTimeoutError
                from pebble.process import Pool

                action_queue = [
                    ('do_update', model, start, end, total, using, self.start_date, self.end_date, self.verbosity)
                    for (start, end) in batches
                ]

                with Pool(workers=self.workers) as pool:
                    jobs = [pool.schedule(worker, args=[action], timeout=self.timeout) for action in action_queue]

                for job in jobs:
                    try:
                        job.get()
                    except PebbleTimeoutError:
                        self.log_error('A worker process timed out')

                # pool.close()
                pool.join()

                # workers resetting connections leads to references to models / connections getting
                # stale and having their connection disconnected from under them. Resetting before
                # the loop continues and it accesses the ORM makes it better.
                db.close_connection()
            else:
                for (start, end) in batches:
                    do_update(backend, index, query_set, start, end, total, self.verbosity)

            if self.remove:
                self.remove_items(backend, batch_size, index, model, True)

            self.log_end(model_set_name)

    def remove_items(self, backend, batch_size, index, model, commit):
        model_set_name = model._meta.verbose_name_plural

        self.log_info('Looking for {} that need pruning...'.format(model_set_name))

        database_pks = list(smart_bytes(pk) for pk in index.index_queryset().values_list('pk', flat=True))

        search_query_set = SearchQuerySet(using=backend.connection_alias).models(model)

        # Retrieve PKs from the index. Note that this cannot be a numeric range query because although
        # pks are normally numeric they can be non-numeric UUIDs or other custom values. To reduce
        # load on the search engine, we only retrieve the pk field, which will be checked against the
        # full list obtained from the database, and the id field, which will be used to delete the
        # record should it be found to be stale.
        all_index_pks = search_query_set.values_list('pk', 'id')

        stale_records = set()
        for start in range(0, search_query_set.count(), batch_size):
            upper_bound = start + batch_size
            for pk, rec_id in all_index_pks[start:upper_bound]:
                if smart_bytes(pk) not in database_pks:
                    stale_records.add(rec_id)

        if stale_records:
            if self.verbosity >= 1:
                self.log_info('  removing {} stale {}...'.format(len(stale_records), model_set_name))

            for pk, rec_id in stale_records:
                if self.verbosity >= 2:
                    self.log_info('    removing {}'.format(rec_id))

                backend.remove(rec_id, commit=commit)

            self.log_info('Done pruning {}'.format(model_set_name))
        else:
            self.log_info('No {} required pruning'.format(model_set_name))
