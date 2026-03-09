import argparse
import re

import redis
from redis.cluster import RedisCluster


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


def redis_cp(src, keys, redis32_and_up=False, dst=None, is_cluster=False):
    """
    :param src: redis connection to src
    :param keys: keys to copied
    :param redis32_and_up: whether both connection are redis32 and up (where we can simply use the 'migrate' command)
    :param dst: redis conneciton to dst (needed if redis32_and_up==False)
    :param is_cluster: whether we're copying to/from cluster
    :return:
    """
    copied = 0
    skipped = 0
    if redis32_and_up and not is_cluster:
        cmd = ['MIGRATE', options.dst, dport, "", ddb, 1000, 'COPY', 'REPLACE', 'KEYS'] + keys
        # print 'cmd', cmd
        if not dryrun:
            src.execute_command(*cmd)
            copied += len(keys)
    else:
        # For clusters or older Redis, use DUMP/RESTORE
        if is_cluster:
            # In cluster mode, we can't use pipelining across different keys/slots
            for key in keys:
                try:
                    ttl = src.pttl(key)
                    if ttl == -2:
                        skipped += 1
                        continue
                    value = src.dump(key)
                    if value is None:
                        skipped += 1
                        continue
                    copied += 1
                    if not dryrun:
                        # dst.delete(key)
                        dst.restore(key, ttl if ttl > 0 else 0, value, replace=True)
                except Exception as e:
                    print(f'Error copying key {key}: {e}')
                    skipped += 1
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
                if ttl == -2 or value is None:
                    skipped += 1
                    continue
                copied += 1
                if not dryrun:
                    dstpipe.restore(key, ttl if ttl > 0 else 0, value, replace=True)
            if not dryrun:
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
    parser.add_argument('--src-cluster', dest='src_cluster', action='store_true', default=False,
                        help='enable cluster mode for src')
    parser.add_argument('--dst-cluster', dest='dst_cluster', action='store_true', default=False,
                        help='enable cluster mode for dst')
    options = parser.parse_args()

    dryrun = options.dryrun
    sdb = options.sdb or options.db
    ddb = options.ddb or options.db
    sport = options.sport or options.port
    dport = options.dport or options.port

    # Determine cluster mode
    src_is_cluster = options.src_cluster
    dst_is_cluster = options.dst_cluster
    is_cluster = src_is_cluster or dst_is_cluster

    # Create connections
    if src_is_cluster:
        src = RedisCluster(host=options.src, port=sport)
    else:
        src = redis.StrictRedis(host=options.src, port=sport, db=sdb)

    if dst_is_cluster:
        dst = RedisCluster(host=options.dst, port=dport)
    else:
        dst = redis.StrictRedis(host=options.dst, port=dport, db=ddb)

    sinfo = src.info() if not src_is_cluster else src.info('server')
    dinfo = dst.info() if not dst_is_cluster else dst.info('server')

    # Get key counts
    if src_is_cluster:
        try:
            nkeys_src = src.dbsize()
        except:
            nkeys_src = 0
    else:
        nkeys_src = sinfo.get('db%d' % sdb, {}).get('keys', 0)

    if dst_is_cluster:
        try:
            nkeys_dst = dst.dbsize()
        except:
            nkeys_dst = 0
    else:
        nkeys_dst = dinfo.get('db%d' % ddb, {}).get('keys', 0)

    version_320 = Version('3.2.0')
    # Get version info (for cluster, get from first node)
    if src_is_cluster:
        first_node_info = list(sinfo.values())[0]
        src_version = Version(first_node_info['redis_version'] if isinstance(first_node_info, dict) else '0.0.0')
    else:
        src_version = Version(sinfo.get('redis_version', '0.0.0'))

    if dst_is_cluster:
        first_node_info = list(dinfo.values())[0]
        dst_version = Version(first_node_info['redis_version'] if isinstance(first_node_info, dict) else '0.0.0')
    else:
        dst_version = Version(dinfo.get('redis_version', '0.0.0'))

    redis32_and_up = src_version > version_320 and dst_version > version_320
    use_migrate = redis32_and_up and not options.nomigrate and not is_cluster

    mode_desc = 'cluster' if is_cluster else 'standalone'
    src_desc = f'{options.src}:{sport}' + ('' if src_is_cluster else f'/{sdb}')
    dst_desc = f'{options.dst}:{dport}' + ('' if dst_is_cluster else f'/{ddb}')

    pattern_desc = f" (pattern: '{options.pattern}')" if options.pattern != '*' else ''
    print('src: %s (%d keys%s) [%s]\ndst: %s (%d keys) [%s]\nvia: %s' % (
        src_desc, nkeys_src, pattern_desc, 'cluster' if src_is_cluster else 'standalone',
        dst_desc, nkeys_dst, 'cluster' if dst_is_cluster else 'standalone',
        'MIGRATE' if use_migrate else 'DUMP/RESTORE'
    ))

    keys = []
    keys_seen = set()
    processed = 0
    total_copied = 0
    total_skipped = 0
    total_matched = 0
    for item in src.scan_iter(match=options.pattern, count=100):
        # Deduplicate in cluster mode
        if is_cluster and item in keys_seen:
            continue
        keys_seen.add(item)
        keys.append(item)
        total_matched += 1
        if len(keys) >= options.batch:
            processed += len(keys)
            if options.verbose:
                print('migrating %d keys ...' % len(keys))
            copied, skipped = redis_cp(src, keys, use_migrate, dst=dst, is_cluster=is_cluster)
            total_copied += copied
            total_skipped += skipped
            keys = []
    if keys:
        processed += len(keys)
        copied, skipped = redis_cp(src, keys, use_migrate, dst=dst, is_cluster=is_cluster)
        total_copied += copied
        total_skipped += skipped

    print('[%s] %d keys copied, %d skipped (matched %d keys with pattern)' % ('DRYRUN' if dryrun else 'DONE', total_copied, total_skipped, total_matched))
