#!/usr/bin/env python

"""
postfix_stats.py
~~~~~~~~

:copyright: (c) 2012 DISQUS.
:license: Apache License 2.0, see LICENSE for more details.
"""

import fileinput
import json
import re
import SocketServer
import sys

from collections import defaultdict
from optparse import OptionParser
from Queue import Queue, Full
from threading import Thread, Lock

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

stats_lock = Lock()

local_emails = {}

line_re = re.compile(r'\A(?P<iso_date>\D{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(?P<source>.+?)\s+(?P<facility>.+?)\[(?P<pid>\d+?)\]:\s(?P<message>.*)\Z')

smtp_re = re.compile(r'\A(?P<message_id>\w+?): to=\<(?P<to_email>.+?)\>, relay=(?P<relay>.+?), (?:conn_use=(?P<conn_use>\d), )?delay=(?P<delay>[0-9\.]+), delays=(?P<delays>[0-9\.\/]+), dsn=(?P<dsn>[0-9\.]+), status=(?P<status>\w+) \((?P<response>.+?)\)\Z')
smtp_set = set(['smtp', 'error'])

local_re = re.compile(r'\A(?P<message_id>\w+?): to=\<(?P<to_email>.+?)\>, orig_to=\<(?P<orig_to_email>.+?)\>, relay=(?P<relay>.+?), delay=(?P<delay>[0-9\.]+), delays=(?P<delays>[0-9\.\/]+), dsn=(?P<dsn>[0-9\.]+), status=(?P<status>\w+) \((?P<response>.+?)\)\Z')
local_set = set(['local'])

smtpd_re = re.compile(r'\A(?P<message_id>\w+?): client=(?P<hostname>.*?)\[(?P<ip>.*?)\]\Z')
smptd_set = set(['smtpd'])

class Parser(Thread):
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
                pass
            finally:
                self.lines.task_done()

    def parse_line(self, line):
        pln =  line_re.match(line)

        if pln:
            pline = pln.groupdict()

            facility = set(pline['facility'].split('/'))

            if not smtp_set.isdisjoint(facility):
                self.parse_smtp(pline['message'])
            elif not local_set.isdisjoint(facility):
                self.parse_local(pline['message'])
            elif not smptd_set.isdisjoint(facility):
                self.parse_smtpd(pline['message'])

    def parse_smtp(self, message):
        global stats

        pmessage = smtp_re.match(message)

        if pmessage:
            pmsg = pmessage.groupdict()

            with stats_lock:
                if '127.0.0.1' in pmsg['relay']:
                    stats['recv']['status'][pmsg['status']] += 1
                    stats['recv']['resp_codes'][pmsg['dsn']] += 1
                else:
                    stats['send']['status'][pmsg['status']] += 1
                    stats['send']['resp_codes'][pmsg['dsn']] += 1

    def parse_local(self, message):
        global stats

        pmessage = local_re.match(message)

        if pmessage:
            pmsg = pmessage.groupdict()

            pemail = local_emails_re.search(pmsg['to_email'])

            if pemail:
                search = pemail.group(1)

                name, count = local_emails[search]

                with stats_lock:
                    stats['local'][name] += 1
                    if count:
                        stats['in']['status'][pmsg['status']] += 1
                        stats['in']['resp_codes'][pmsg['dsn']] += 1

    def parse_smtpd(self, message):
        global stats

        pmessage = smtpd_re.match(message)

        if pmessage:
            pmsg = pmessage.groupdict()

            with stats_lock:
                stats['clients'][pmsg['ip']] += 1

class ParserPool(object):
    def __init__(self, num_parsers):
        self.lines = Queue(num_parsers*1000)
        [Parser(self.lines) for _ in xrange(num_parsers)]

    def add_line(self, line):
        self.lines.put_nowait(line)

    def join(self):
        self.lines.join()

class CommandHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        command = self.rfile.readline()

        if command.lower() == 'stats':
            self.wfile.write(json.dumps(stats))
        elif command.lower() == 'prettystats':
            self.wfile.write(json.dumps(stats, indent=2))

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

if __name__ == '__main__':
    usage = "usage: %prog [options] file1 file2 ... fileN"
    opt_parser = OptionParser(usage)
    opt_parser.add_option("-d", "--daemon", dest="daemon", default=False, action="store_true",
        help="Run tcp server for getting stats from")
    opt_parser.add_option("-p", "--port", dest="port", default="7777", type="int",
        help="Port to listen on for grabbing stats", metavar="PORT")
    opt_parser.add_option("-i", "--host", dest="host", default="127.0.0.1",
        help="Host/IP to listen on for grabbing stats", metavar="HOST")
    opt_parser.add_option("-c", "--concurrency", dest="num_threads", default="2", type="int",
        help="Number of threads to spawn for handling lines", metavar="NUM")
    opt_parser.add_option("-l", "--local", dest="local_emails", action="append",
        help="Search for STRING in incoming email addresses and incr stat NAME and if COUNT, count in incoming - STRING,NAME,COUNT", metavar="LOCAL_TUPLE")

    (options, args) = opt_parser.parse_args()

    true_values = ['yes', '1', 'true']
    for local_email in options.local_emails:
        try:
            search, name, count = local_email.strip('()').split(',')
        except ValueError:
            print local_email
            print "Require the full 3 fields in LOCAL_TUPLE"
            sys.exit(1)

        local_emails[search] = (name, count.lower() in true_values)
    
    local_emails_re = re.compile(r'(%s)' % '|'.join(local_emails.keys()))

    if options.daemon:
        server = ThreadedTCPServer((options.host, options.port), CommandHandler)
        server_thread = Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

    parser_pool = ParserPool(options.num_threads)

    for line in fileinput.input(args):
        try:
            parser_pool.add_line(line.strip('\n'))
        except Full:
            # Dont really care
            pass

    parser_pool.join()

    if not options.daemon:
        print json.dumps(stats, indent=2)
    else:
        server.shutdown()

    sys.exit(0)

