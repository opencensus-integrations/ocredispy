# Copyright 2019, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ocredis
import redis

def test_methods_on_redis_also_exist_on_ocredis():
    methods = [
        'append', 'bgrewriteaof', 'bgsave', 'bitcount', 'bitfield', 'bitop', 'bitpos', 'blpop', 'brpop', 'brpoplpush',
        'bzpopmax', 'bzpopmin', 'client_getname', 'client_id', 'client_kill', 'client_kill_filter', 'client_list',
        'client_pause', 'client_setname', 'client_unblock', 'cluster', 'config_get', 'config_resetstat', 'config_rewrite',
        'config_set', 'dbsize', 'debug_object', 'decr', 'decrby', 'delete', 'dump', 'echo', 'eval', 'evalsha', 'execute_command',
        'exists', 'expire', 'expireat', 'flushall', 'flushdb', 'from_url', 'geoadd', 'geodist', 'geohash', 'geopos', 'georadius',
        'georadiusbymember', 'get', 'getbit', 'getrange', 'getset', 'hdel', 'hexists', 'hget', 'hgetall', 'hincrby', 'hincrbyfloat',
        'hkeys', 'hlen', 'hmget', 'hmset', 'hscan', 'hscan_iter', 'hset', 'hsetnx', 'hstrlen', 'hvals', 'incr', 'incrby',
        'incrbyfloat', 'info', 'keys', 'lastsave', 'lindex', 'linsert', 'llen', 'lock', 'lpop', 'lpush', 'lpushx', 'lrange',
        'lrem', 'lset', 'ltrim', 'memory_purge', 'memory_usage', 'mget', 'migrate', 'move', 'mset', 'msetnx', 'object',
        'parse_response', 'persist', 'pexpire', 'pexpireat', 'pfadd', 'pfcount', 'pfmerge', 'ping', 'pipeline', 'psetex', 'pttl',
        'publish', 'pubsub', 'pubsub_channels', 'pubsub_numpat', 'pubsub_numsub', 'randomkey', 'register_script',
        'rename', 'renamenx', 'restore', 'rpop', 'rpoplpush', 'rpush', 'rpushx', 'sadd', 'save', 'scan', 'scan_iter', 'scard',
        'script_exists', 'script_flush', 'script_kill', 'script_load', 'sdiff', 'sdiffstore', 'sentinel',
        'sentinel_get_master_addr_by_name', 'sentinel_master', 'sentinel_masters', 'sentinel_monitor', 'sentinel_remove',
        'sentinel_sentinels', 'sentinel_set', 'sentinel_slaves', 'set', 'set_response_callback', 'setbit', 'setex', 'setnx',
        'setrange', 'shutdown', 'sinter', 'sinterstore', 'sismember', 'slaveof', 'slowlog_get', 'slowlog_len', 'slowlog_reset',
        'smembers', 'smove', 'sort', 'spop', 'srandmember', 'srem', 'sscan', 'sscan_iter', 'strlen', 'substr', 'sunion',
        'sunionstore', 'swapdb', 'time', 'touch', 'transaction', 'ttl', 'type', 'unlink', 'unwatch', 'wait', 'watch', 'xack',
        'xadd', 'xclaim', 'xdel', 'xgroup_create', 'xgroup_delconsumer', 'xgroup_destroy', 'xgroup_setid', 'xinfo_consumers',
        'xinfo_groups', 'xinfo_stream', 'xlen', 'xpending', 'xpending_range', 'xrange', 'xread', 'xreadgroup', 'xrevrange',
        'xtrim', 'zadd', 'zcard', 'zcount', 'zincrby', 'zinterstore', 'zlexcount', 'zpopmax', 'zpopmin', 'zrange', 'zrangebylex',
        'zrangebyscore', 'zrank', 'zrem', 'zremrangebylex', 'zremrangebyrank', 'zremrangebyscore', 'zrevrange', 'zrevrangebylex',
        'zrevrangebyscore', 'zrevrank', 'zscan', 'zscan_iter', 'zscore', 'zunionstore']

    for method_name in methods:
        method_object = getattr(ocredis.OcRedis, method_name)
        assert method_object != None
        assert hasattr(method_object, '__call__')

    # Now for the rigorous check.
    redis_client_namespace_variables = dir(redis.Redis)
    for redis_client_namespace_variable in redis_client_namespace_variables:
        ocredis_attr = getattr(ocredis.OcRedis, redis_client_namespace_variable)
        assert ocredis_attr != None
