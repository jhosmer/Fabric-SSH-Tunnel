#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SCP Utility with optional SSH Tunneling

Usage:

    fab_sync.py [-h] -l LOCAL -r REMOTE [-e {qa,prod}] [-t]
    
    optional arguments:
        -h, --help            show this help message and exit
        -l LOCAL, --local-dir LOCAL
                              Local dir root
        -r REMOTE, --remote-dir REMOTE
                              Remote dest dir
        -e {qa,prod}, --environment {qa,prod}
                              Environment
        -t, --tunnel          Sync through an SSH tunnel

"""

__docformat__ = 'restructuredtext'

import os
import sys
import logging
import argparse
import subprocess
import shlex
import time
import fabric.api as fabapi

fabapi.env.update({
    'abort_on_prompts': True,
    'always_use_pty': False,
    'combine_stderr': False,
    'command_timeout': 900,
    'disable_known_hosts': True,
    'key_filename': ['~/.ssh/id_rsa'],
    'parallel': True,
    'quiet': True,
    'timeout': 900,
    'user': XXXXXX,
    'warn_only': True,
})

GATEWAY_HOST = XXXXXXX.com
GATEWAY_USER = XXXXXX
GATEWAY_PORT = 4204

class SSHTunnel(object):
    def __init__(self, bridge_user, bridge_host, dest_host, dest_port=22, local_port=GATEWAY_PORT):
        self.local_port = local_port
        cmd = 'ssh -Nqttv -oStrictHostKeyChecking=no -L {}:{}:{} {}@{}'.format(
            local_port, dest_host, dest_port, bridge_user, bridge_host)
        self.p = subprocess.Popen(shlex.split(cmd))
        time.sleep(2)

    def __del__(self):
        self.p.kill()

    def __str__(self):
        return ':'.join(('localhost', str(self.local_port)))

    def gethost(self):
        return str(self)


def xfer(local, remote, hosts, tunnel=False):
    """
    Transfer files

    :param str local: Local Dir
    :param str remote: Remote Dir
    :param list[str] hosts: Host names to transfer files to
    :param bool tunnel: Transfer through ssh tunnel
    :returns: Success Status
    :rtype: int
    """
    def _put(l, r):
        return fabapi.put(local_path=l, remote_path=r, mirror_local_mode=True).succeeded
    tunnels = []
    if tunnel:
        for i, host in enumerate(hosts):
            t = SSHTunnel(GATEWAY_USER, GATEWAY_HOST, host, local_port=GATEWAY_PORT+i)
            tunnels.append(t)
        hosts = [t.gethost() for t in tunnels]
    ret = fabapi.execute(_put, l=local, r=remote, hosts=hosts)
    del tunnels
    return all(ret.values())


def main(args=None):
    """
    Sync

    :param list[str] args: Args
    """
    config = parse_args(args)
    if config.ENV == 'qa':
        hostlist = ['qatools'+str(_) for _ in xrange(1, 4)]
    elif config.ENV == 'prod':
        hostlist = ['prodtools3']
    else:
        raise ValueError('Unknown environment: {}'.format(config.ENV))
    return not xfer(config.LOCAL, config.REMOTE, hostlist, config.TUNNEL)


def parse_args(args=None):
    """
    Parses command line arguments into a Namespace

    :param list[str] args: (optional) List of string arguments to parse.  If
                           ommitted or `None` will parse `sys.argv`
    :return: Parsed Arguments
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-e', '--environment', dest='ENV', help='Environment', choices=['qa', 'prod'], default='qa')
    parser.add_argument('-l', '--local-dir', dest='LOCAL', help='Local dir root', required=True)
    parser.add_argument('-r', '--remote-dir', dest='REMOTE', help='Remote dest dir', required=True)
    parser.add_argument('-t', '--tunnel', action='store_true', default=False, dest='TUNNEL',
                        help='Sync through an SSH tunnel')
    parsed_args = parser.parse_args(args=args, namespace=util.BetterNamespace())
    parsed_args.LOCAL = os.path.abspath(parsed_args.LOCAL)
    if not os.path.exists(parsed_args.LOCAL):
        parser.error('{}: No such file or directory'.format(parsed_args.LOCAL))
    return parsed_args

if __name__ == '__main__':
    sys.exit(main())
