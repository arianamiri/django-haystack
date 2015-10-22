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
from haystack.exceptions import ConnectionError, HaystackError
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

        try:
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
        except:
            raise HaystackError('DB connection issue')

        model_set_name = model._meta.verbose_name_plural

        backend = haystack_connections[using].get_backend()
        index = haystack_connections[using].get_unified_index().get_index(model)
        query_set = index.build_queryset(start_date=start_date, end_date=end_date)

        try:
            do_update(backend, index, query_set, start, end, total, verbosity=verbosity)
        except Exception as e:
            raise HaystackError('DO_UPDATE issue ({})'.format(e.message))

        time_end_batch = default_timer()

        elapsed = timedelta(seconds=(time_end_batch - time_start_batch))

        print('[Haystack]   < Finished a {} batch. It took {} ({:.2f}s) total. [{}]'.format(
            model_set_name,
            format_timedelta(elapsed),
            elapsed.total_seconds(),
            '{} - {} ({})'.format(start + 1, end, total)
        ))
    except HaystackError as he:
        print()
        print('[Haystack] JOB FAIL: {}'.format(he.message))
        traceback.print_stack()
        print()
        raise

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
        base_text = '[Haystack]  > Indexing {1:>{0}} - {2:>{0}} (of {3:>{0}})'.format(len(str(total)), start + 1, end, total)
        if hasattr(os, 'getppid') and os.getpid() == os.getppid():
            print(base_text)
        else:
            print('{} [PID {}]'.format(base_text, os.getpid()))

    try:
        backend.update(index, current_qs, commit=False)
    except Exception as e:
        print()
        print('[Haystack] !! { backend.update } threw:')
        print('-----')
        print(e)
        print('-----')
        traceback.print_stack()
        print()
        raise HaystackError('[Haystack] !! { backend.update } issue: {}'.format(e.message))

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

        self.timers = defaultdict(dict)

        return super(Command, self).handle(*items, **options)

    def log_info(self, message, header=None, footer=None):
        if self.verbosity < 1:
            return

        if header:
            self.logger.info('[Haystack] {}'.format(len(message) * header))
        self.logger.info('[Haystack] {}'.format(message))
        if footer:
            self.logger.info('[Haystack] {}'.format(len(message) * footer))

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

    def log_start_header(self, model_set_name):
        self.log_start('-- Updating all {} --'.format(model_set_name), timer_name='default', header='=', footer='-')

    def log_start(self, message, timer_name='default', header=None, footer=None):
        if self.verbosity < 1:
            return

        self.timers[timer_name]['start'] = default_timer()

        self.log_info(message, header=header, footer=footer)

    def log_end(self, message, timer_name='default'):
        if self.verbosity < 1:
            return

        elapsed = timedelta(seconds=(default_timer() - self.timers[timer_name]['start']))

        self.log_info('{} It took {} ({:.2f}s) total.'.format(
            message,
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

            self.log_start_header(model_set_name)

            query_set = index.build_queryset(using=using, start_date=self.start_date,
                                      end_date=self.end_date)

            total = query_set.count()

            if self.verbosity >= 1:
                self.log_info('> Indexing {} {}...'.format(total, force_text(model_set_name)))

            batch_size = self.batchsize or backend.batch_size

            batches = ((start, min(start + batch_size, total)) for start in range(0, total, batch_size))

            if self.workers:
                import multiprocessing

                action_queue = [
                    ('do_update', model, start, end, total, using, self.start_date, self.end_date, self.verbosity)
                    for (start, end) in batches
                ]

                pool = multiprocessing.Pool(self.workers)
                pool.map(worker, action_queue)
                pool.terminate()

                # workers resetting connections leads to references to models / connections getting
                # stale and having their connection disconnected from under them. Resetting before
                # the loop continues and it accesses the ORM makes it better.
                db.close_connection()
            else:
                for (start, end) in batches:
                    do_update(backend, index, query_set, start, end, total, self.verbosity)

            self.log_end(' < Finished updating backend for {}.'.format(model_set_name))

            if self.remove:
                self.remove_items(backend, batch_size * 4, index, model, True)

    def get_batches(self, data_set, batch_size):
        return (set(data_set[start:start + batch_size]) for start in range(0, len(data_set), batch_size))

    def remove_items(self, backend, batch_size, index, model, commit):
        model_set_name = model._meta.verbose_name_plural
        search_query_set = SearchQuerySet(using=backend.connection_alias).models(model)

        self.log_start('> Looking for {} that need pruning...'.format(model_set_name), timer_name='collate')

        database_pks = set(map(str, index.index_queryset().values_list('pk', flat=True)))

        # Grab the PKs from the index in batches for memory reasons
        all_them = set().union(*list(self.get_batches(search_query_set.values_list('pk', flat=True), batch_size)))

        stale_record_pks = all_them - database_pks

        self.log_end('< Done collating stale records', timer_name='collate')

        if stale_record_pks:
            rec_ids = set(search_query_set.filter(django_id__in=stale_record_pks).values_list('id', flat=True))

            if self.verbosity >= 1:
                self.log_info(' > removing {} stale {}...'.format(len(stale_record_pks), model_set_name))

            for rec_id in rec_ids:
                if self.verbosity >= 2:
                    self.log_info('  < removing {}'.format(rec_id))

                backend.remove(rec_id, commit=commit)

            self.log_info(' < Done pruning {}'.format(model_set_name))
        else:
            self.log_info(' < No {} required pruning'.format(model_set_name))
