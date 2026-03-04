
In operation, we may need to copy one redis db to another. rdb is an awesome
mechanism but if you're migrating things in/out of services like ElastiCache.
It's just not on-the-fly.

Other people suggested some good approaches. Here are some ready-to-use codes
in python.


## Common Usages
```
# copy redis1/db0 -> redis2/db0 (dryrun)
redis-cp.py redis1 redis2 --dryrun --verbose

# copy redis1/db3 ->redis2/db3 (dryrun)
redis-cp.py redis1 redis2 --dryrun --verbose --db 3

# copy redis1/db1 ->redis2/db3 (dryrun)
redis-cp.py redis1 redis2 --dryrun --verbose --sdb 1 --ddb 3
```

## Advanced Usages
```
# running two redis servers locally (different port) for testing/develop
redis-cp.py redis1 redis2 --sport 6379 --dport 7777

# change batch size (default is 100, which is just a random pick)
redis-cp.py redis1 redis2 --batch 500
```

## Cluster Mode Support
```
# copy from cluster to cluster
redis-cp.py redis-cluster1 redis-cluster2 --src-cluster --dst-cluster

# copy from standalone to cluster
redis-cp.py redis-standalone redis-cluster --dst-cluster

# copy from cluster to standalone
redis-cp.py redis-cluster redis-standalone --src-cluster
```
