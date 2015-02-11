#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='postfix-stats',
    version='0.3.2',
    author='DISQUS',
    author_email='opensource@disqus.com',
    url='http://github.com/disqus/postfix-stats',
    description = 'Simple threaded stats aggregator for Postfix. When running as a syslog destination, it can be used to get realtime cumulative stats.',
    zip_safe=False,
    scripts=['postfix_stats.py'],
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Topic :: Software Development',
        'Topic :: Communications :: Email :: Mail Transport Agents',
        'Topic :: System :: Monitoring',
    ],
    keywords='postfix stats',
)

