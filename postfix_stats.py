#!/usr/bin/env python

"""
postfix_stats.py
~~~~~~~~

:copyright: (c) 2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import fileinput
import json
import logging
import re
import SocketServer
import sys

from collections import defaultdict, Iterator
from optparse import OptionParser
from Queue import Queue, Full
from threading import Thread, Lock

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('postfix_stats')

handlers = defaultdict(list)

stats_lock = Lock()

stats = {}
stats['recv'] = {
    'status': defaultdict(int),
    'resp_codes': defaultdict(int),
}
stats['send'] = {
    'status': defaultdict(int),
    'resp_codes': defaultdict(int),
}
stats['in'] = {
    'status': defaultdict(int),
    'resp_codes': defaultdict(int),
}
stats['clients'] = defaultdict(int)
stats['local'] = defaultdict(int)

local_addresses = {}


class Handler(object):
    filter_re = re.compile(r'(?!)')
    facilities = None

    def __init__(self, *args, **kwargs):
        assert isinstance(self.filter_re, re._pattern_type)
        self.facilities = set(self.facilities)

        self.register(self.facilities)

    @classmethod
    def parse(self, line):
        pline = self.filter_re.match(line)

        if pline:
            logger.debug(pline.groupdict())
            self.handle(**pline.groupdict())

    @classmethod
    def handle(self, **kwargs):
        raise NotImplementedError()

    def register(self, facilities):
        facilities = set(facilities)
        for facility in facilities:
            if self not in handlers[facility]:
                handlers[facility].append(self)

        self.facilities |= facilities


class BounceHandler(Handler):
    facilities = set(['bounce'])
    filter_re = re.compile((r'\A(?P<message_id>\w+?): sender non-delivery notification: (?P<bounce_message_id>\w+?)\Z'))

    @classmethod
    def handle(self, message_id=None, bounce_message_id=None):
        pass


class CleanupHandler(Handler):
    facilities = set(['cleanup'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): message-id=\<(?P<ext_message_id>.+?)\>\Z')

    @classmethod
    def handle(self, message_id=None, ext_message_id=None):
        pass


class LocalHandler(Handler):
    facilities = set(['local'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): to=\<(?P<to_email>.*?)\>, orig_to=\<(?P<orig_to_email>.*?)\>, relay=(?P<relay>.+?), delay=(?P<delay>[0-9\.]+), delays=(?P<delays>[0-9\.\/]+), dsn=(?P<dsn>[0-9\.]+), status=(?P<status>\w+) \((?P<response>.+?)\)\Z')
    local_addresses_re = re.compile(r'(?!)')

    def __init__(self, local_addresses_re=None, *args, **kwargs):
        super(LocalHandler, self).__init__(*args, **kwargs)

        if local_addresses_re:
            assert isinstance(local_addresses_re, re._pattern_type)
            self.__class__.local_addresses_re = local_addresses_re

    @classmethod
    def handle(self, message_id=None, to_email=None, orig_to_email=None, relay=None, delay=None, delays=None, dsn=None, status=None, response=None):
        pemail = self.local_addresses_re.search(to_email)

        if pemail:
            search = pemail.group(1)

            name, count = local_addresses[search]

            logger.debug('Local address <%s> count (%s) as "%s"', search, count, name)

            with stats_lock:
                stats['local'][name] += 1
                if count:
                    stats['in']['status'][status] += 1
                    stats['in']['resp_codes'][dsn] += 1


class QmgrHandler(Handler):
    facilities = set(['qmgr'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): (?:(?P<removed>removed)|(?:from=\<(?P<from_address>.*?)\>, size=(?P<size>[0-9]+), nrcpt=(?P<nrcpt>[0-9]+) \(queue (?P<queue>[a-z]+)\)))?\Z')

    @classmethod
    def handle(self, message_id=None, removed=None, from_address=None, size=None, nrcpt=None, queue=None):
        pass


class SmtpHandler(Handler):
    facilities = set(['smtp', 'error'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): to=\<(?P<to_email>.+?)\>, relay=(?P<relay>.+?), (?:conn_use=(?P<conn_use>\d), )?delay=(?P<delay>[0-9\.]+), delays=(?P<delays>[0-9\.\/]+), dsn=(?P<dsn>[0-9\.]+), status=(?P<status>\w+) \((?P<response>.+?)\)\Z')

    @classmethod
    def handle(self, message_id=None, to_email=None, relay=None, conn_use=None, delay=None, delays=None, dsn=None, status=None, response=None):
        with stats_lock:
            stat = 'recv' if '127.0.0.1' in relay else 'send'
            stats[stat]['status'][status] += 1
            stats[stat]['resp_codes'][dsn] += 1


class SmtpdHandler(Handler):
    facilities = set(['smtpd'])
    filter_re = re.compile(r'\A(?P<message_id>\w+?): client=(?P<client_hostname>[.\w-]+)\[(?P<client_ip>[A-Fa-f0-9.:]{3,39})\](?:, sasl_method=[\w-]+)?(?:, sasl_username=[-_.@\w]+)?(?:, sasl_sender=\S)?(?:, orig_queue_id=\w+)?(?:, orig_client=(?P<orig_client_hostname>[.\w-]+)\[(?P<orig_client_ip>[A-Fa-f0-9.:]{3,39})\])?\Z')

    @classmethod
    def handle(self, message_id=None, client_hostname=None, client_ip=None, orig_client_hostname=None, orig_client_ip=None):
        ip = orig_client_ip or client_ip
        with stats_lock:
            stats['clients'][ip] += 1


class Parser(Thread):
    line_re = re.compile(r'\A(?P<iso_date>\D{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(?P<source>.+?)\s+(?P<facility>.+?)\[(?P<pid>\d+?)\]:\s(?P<message>.*)\Z')

    def __init__(self, lines):
        super(Parser, self).__init__()
        self.lines = lines

        self.daemon = True
        self.start()

    def run(self):
        while True:
            line = self.lines.get()

            try:
                self.parse_line(line)
            except Exception, e:
                logger.exception('Error parsing line: %s', line)
            finally:
                self.lines.task_done()

    def parse_line(self, line):
        pln = self.line_re.match(line)

        if pln:
            pline = pln.groupdict()
            logger.debug(pline)

            facility = pline['facility'].split('/')

            for handler in handlers[facility[-1]]:
                handler.parse(pline['message'])


class ParserPool(object):
    def __init__(self, num_parsers):
        self.lines = Queue(num_parsers * 1000)

        for i in xrange(num_parsers):
            logger.info('Starting parser %s', i)
            Parser(self.lines)

    def add_line(self, line, block=False):
        self.lines.put(line, block)

    def join(self):
        self.lines.join()


class CommandHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        command = self.rfile.readline().strip()

        logger.info('Got command: %s', command)

        if command.lower() == 'stats':
            self.wfile.write(json.dumps(stats))
        elif command.lower() == 'prettystats':
            self.wfile.write(json.dumps(stats, indent=2))


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class StdinReader(Iterator):
    def next(self):
        try:
            line = sys.stdin.readline()
        except KeyboardInterrupt:
            raise StopIteration

        if not line:
            raise StopIteration

        return line

    def isstdin(self):
        return True


def main(logs, daemon=False, host='127.0.0.1', port=7777, concurrency=2, local_emails=None, **kwargs):
    true_values = ['yes', '1', 'true']
    for local_email in local_emails:
        try:
            search, name, count = local_email.strip('()').split(',')
        except ValueError:
            logger.error('LOCAL_TUPLE requires 3 fields: %s', local_email)
            return -1

        local_addresses[search] = (name, count.lower() in true_values)

    if local_addresses:
        local_addresses_re = re.compile(r'(%s)' % '|'.join(local_addresses.keys()))
        logger.debug('Local email pattern: %s', local_addresses_re.pattern)
    else:
        local_addresses_re = re.compile(r'(?!)')

    handlers = (LocalHandler(local_addresses_re), SmtpHandler(), SmtpdHandler())

    if daemon:
        server = ThreadedTCPServer((host, port), CommandHandler)
        server_thread = Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        logger.info('Listening on %s:%s', host, port)

    parser_pool = ParserPool(concurrency)

    if not logs or logs[0] is '-':
        reader = StdinReader()
    else:
        reader = fileinput.input(logs)

    for line in reader:
        try:
            parser_pool.add_line(line.strip('\n'), not reader.isstdin())
        except Full:
            logger.warning('Line parser queue full')
            # Dont really care
            pass

    parser_pool.join()

    if not daemon:
        print json.dumps(stats, indent=2)
    else:
        server.shutdown()

    return 0

if __name__ == '__main__':
    usage = "usage: %prog [options] file1 file2 ... fileN"
    opt_parser = OptionParser(usage)
    opt_parser.add_option("-v", "--verbose", dest="verbosity", default=0, action="count",
        help="-v for a little info, -vv for debugging")
    opt_parser.add_option("-d", "--daemon", dest="daemon", default=False, action="store_true",
        help="Run tcp server for getting stats from")
    opt_parser.add_option("-p", "--port", dest="port", default=7777, type="int",
        help="Port to listen on for grabbing stats", metavar="PORT")
    opt_parser.add_option("-i", "--host", dest="host", default="127.0.0.1",
        help="Host/IP to listen on for grabbing stats", metavar="HOST")
    opt_parser.add_option("-c", "--concurrency", dest="concurrency", default=2, type="int",
        help="Number of threads to spawn for handling lines", metavar="NUM")
    opt_parser.add_option("-l", "--local", dest="local_emails", default=[], action="append",
        help="Search for STRING in incoming email addresses and incr stat NAME and if COUNT, count in incoming - STRING,NAME,COUNT", metavar="LOCAL_TUPLE")

    (options, args) = opt_parser.parse_args()

    if options.verbosity == 1:
        logger.setLevel(logging.INFO)
    elif options.verbosity == 2:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.ERROR)

    sys.exit(main(args, **options.__dict__))
