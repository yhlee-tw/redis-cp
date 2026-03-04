import argparse
import re

import redis


class Version(object):
    RE_XYZ = re.compile('^(\d+)\.(\d+)\.(\d+)')
    RE_VXYZ = re.compile('^v(\d+)\.(\d+)\.(\d+)')

    def __init__(self, str):
        self.raw = str or '?'
        self.valid = False

        for matching in [Version.RE_XYZ, Version.RE_VXYZ]:
            match = matching.match(self.raw)
            if match:
                self.digits = [int(d) for d in match.groups()]
                self.valid = True
                break
        if not self.valid:
            self.digits = [0, 0, 0]
        self.version = '%d.%d.%d' % tuple(self.digits)

    def __eq__(self, other):
        return self.digits == other.digits

    def __lt__(self, other):
        return self.digits < other.digits

    def __le__(self, other):
        return self.digits <= other.digits

    def __gt__(self, other):
        return self.digits > other.digits

    def __ge__(self, other):
        return self.digits >= other.digits

    def __ne__(self, other):
        return self.digits != other.digits


def redis_cp(src, keys, redis32_and_up=False, dst=None):
    """
    :param src: redis connection to src
    :param keys: keys to copied
    :param redis32_and_up: whether both connection are redis32 and up (where we can simply use the 'migrate' command)
    :param dst: redis conneciton to dst (needed if redis32_and_up==False)
    :return:
    """
    copied = 0
    skipped = 0
    if redis32_and_up:
        cmd = ['MIGRATE', options.dst, dport, "", ddb, 1000, 'COPY', 'REPLACE', 'KEYS'] + keys
        # print 'cmd', cmd
        if not dryrun:
            src.execute_command(*cmd)
            copied += len(keys)
    else:
        pipe = src.pipeline(transaction=False)
        for key in keys:
            pipe.pttl(key)  # restore needs msec
            pipe.dump(key)
        data = pipe.execute()
        tuples = [[keys[i // 2], data[i], data[i + 1]] for i in range(0, len(data), 2)]

        dstpipe = dst.pipeline(transaction=False)
        for tup in tuples:
            key, ttl, value = tup
            if ttl == -2:
                skipped += 1
                continue
            copied += 1
            if not dryrun:
                dstpipe.delete(key)
                dstpipe.restore(key, ttl if ttl > 0 else 0, value)
        dstpipe.execute()
    return copied, skipped


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''
    Copy redis keys from db to another.
    
    Internally it uses MIGRATE (if both redis server versions are > 3.2) or DUMP/RESTORE.
    TTL is preserved. And batch/pipeline are used.

    ''')
    parser.add_argument('src', help='redis src host')
    parser.add_argument('dst', help='redis dst host')
    parser.add_argument('--dryrun', dest='dryrun', action='store_true', default=False)
    parser.add_argument('--db', dest='db', type=int, default=0, help='db to copy')
    parser.add_argument('--port', dest='port', type=int, default=6379, help='port')
    parser.add_argument('--sdb', dest='sdb', type=int, help='src db, if different from <db>')
    parser.add_argument('--ddb', dest='ddb', type=int, help='dst db, if different from <db>')
    parser.add_argument('--sport', dest='sport', type=int, help='src port, if different from <port>')
    parser.add_argument('--dport', dest='dport', type=int, help='dst port, if different from <port>')
    parser.add_argument('--batch', dest='batch', type=int, default=100, help='batch size')
    parser.add_argument('--verbose', dest='verbose', action='store_true', default=False)
    parser.add_argument('--pattern', dest='pattern', default='*')
    parser.add_argument('--nomigrate', dest='nomigrate', action='store_true', default=False)
    options = parser.parse_args()

    dryrun = options.dryrun
    sdb = options.sdb or options.db
    ddb = options.ddb or options.db
    sport = options.sport or options.port
    dport = options.dport or options.port
    src = redis.StrictRedis(host=options.src, port=sport, db=sdb)
    dst = redis.StrictRedis(host=options.dst, port=dport, db=ddb)

    sinfo = src.info()
    dinfo = dst.info()
    nkeys_src = sinfo.get('db%d' % sdb, {}).get('keys', 0)
    nkeys_dst = dinfo.get('db%d' % ddb, {}).get('keys', 0)
    version_320 = Version('3.2.0')
    redis32_and_up = Version(sinfo.get('redis_version')) > version_320 and \
                     Version(dinfo.get('redis_version')) > version_320
    use_migrate = redis32_and_up and not options.nomigrate

    print('src: %s:%d/%d (%d keys)\ndst: %s:%d/%d (%d keys)\nvia: %s' % (
        options.src, sport, sdb, nkeys_src,
        options.dst, dport, ddb, nkeys_dst,
        'MIGRATE' if use_migrate else 'DUMP/RESTORE'
    ))

    keys = []
    processed = 0
    total_copied = 0
    total_skipped = 0
    for item in src.scan_iter(match=options.pattern, count=100):
        keys.append(item)
        if len(keys) >= options.batch:
            processed += len(keys)
            if options.verbose:
                print('migrating %d/%d keys ...' % (processed, nkeys_src))
            copied, skipped = redis_cp(src, keys, use_migrate, dst=dst)
            total_copied += copied
            total_skipped += skipped
            keys = []
    if keys:
        processed += len(keys)
        copied, skipped = redis_cp(src, keys, use_migrate, dst=dst)
        total_copied += copied
        total_skipped += skipped

    print('[%s] %d keys copied, %d skipped' % ('DRYRUN' if dryrun else 'DONE', total_copied, total_skipped))
