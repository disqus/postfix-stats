postfix-stats.py
================

Simple threaded stats aggregator for Postfix. When running as a syslog destination, it can be used to get realtime cumulative stats.

Options
-------

Usage: postfix_stats.py [options] file1 file2 ... fileN

-d, --daemon                Run tcp server for getting stats from
-p PORT, --port=PORT        Port to listen on for grabbing stats (Default: 7777)
-i HOST, --host=HOST        Host/IP to listen on for grabbing stats (Default: 127.0.0.1)
-c NUM, --concurrency=NUM   Number of threads to spawn for handling lines (Default: 2)
-l LOCAL_TUPLE, --local=LOCAL_TUPLE
                            Search for STRING in incoming email addresses and incr stat NAME
                            and if COUNT, count in incoming stats - STRING,NAME,COUNT

Daemon Mode
-----------

Primary use is as a syslog destination

**syslog-ng**

::

    destination df_postfix_stats { program("/usr/bin/python /usr/bin/postfix_stats.py -d -c 4 -"); };
    filter f_postfix { program("^postfix/"); };
    log {
        source(src);
        filter(f_mail);
        filter(f_postfix);
        destination(df_postfix_stats);
    };

**syslog**

::

    mail.*  |/usr/bin/python /usr/bin/postfix_stats.py -d -c 4 -

**rsyslog**

Create ``postfix_stats.sh`` that calls postfix_stats.py with the arguments you wish to use

::

    $ModLoad omprog
    $actionomprogbinary /usr/bin/postfix_stats.sh
    :syslogtag, startswith, "postfix" :omprog:;RSYSLOG_TraditionalFileFormat

**tail**

If you don't want to wire it up to syslog but need current stats use this.

::

    tail -qf /var/log/mail.* | /usr/bin/postfix_stats.py -d


To grab the current cumulative stats as a json dump

    echo stats | nc 127.0.0.1 7777

Or human readable

    echo prettystats | nc 127.0.0.1 7777

File Mode
---------

Pass it a list of log files or stdin and it will aggregate them and dump a json dictionary

    postfix_stats.py -c 12 -l disqus.net,notifications,true postfix.log postfix.log.1 postfix.log.2

