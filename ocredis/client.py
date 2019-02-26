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

import redis

try:
    from ocredis.observability import trace_and_record_stats_with_key_and_value
except ImportError:
    from .ocredis.observability import trace_and_record_stats_with_key_and_value
except Exception as e:
    raise e

class OcRedis(redis.Redis):
    """
    OcRedis is the instrumented wrapper for redis.Redis clients.
    It provides distributed traces and metrics using OpenCensus.
    """

    def append(self, key, value, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.append',
                super(OcRedis, self).append, key, value, *args, **kwargs)

    def bgrewriteaof(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bgrewriteaof',
                super(OcRedis, self).bgrewriteaofget, None, None)

    def bgsave(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bgsave',
                super(OcRedis, self).bgsave, None, None)

    def bitcount(self, key, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bgsave',
                super(OcRedis, self).bgsave, key, None, key, *args, **kwargs)

    def bitfield(self, key, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bitfield',
                super(OcRedis, self).bitfield, key, *args, **kwargs)

    def bitop(self, operation, dest, *keys):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bitop',
                super(OcRedis, self).bitop, operation, None, operation, dest, *keys)

    def blpop(self, keys, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.blpop',
                super(OcRedis, self).blpop, keys, None, keys, *args, **kwargs)

    def brpop(self, keys, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.brpop',
                super(OcRedis, self).brpop, keys, None, keys, *args, **kwargs)

    def brpoplpush(self, src, dst, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.brpoplpush',
                super(OcRedis, self).brpoplpush, src, dst, src, ds, *args, **kwargs)

    def bzpopmax(self, keys, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bzpopmax',
                super(OcRedis, self).bzpopmax, keys, None, keys, *args, **kwargs)

    def bzpopmin(self, keys, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.bzpopmin',
                super(OcRedis, self).bzpopmin, keys, None, keys, *args, **kwargs)

    def client_getname(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_getname',
                super(OcRedis, self).client_getname, None, None)

    def client_id(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_id',
                super(OcRedis, self).client_id, None, None)

    def client_kill(self, address):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_kill',
                super(OcRedis, self).client_kill, address, None, address)

    def client_kill_filter(self, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_kill_filter',
                super(OcRedis, self).client_kill_filter, None, None, *args, **kwargs)

    def client_list(self, _type=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_list',
                super(OcRedis, self).client_list, None, None, _type)

    def client_pause(self, timeout):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_pause',
                super(OcRedis, self).client_pause, None, None, timeout)

    def client_setname(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.client_setname',
                super(OcRedis, self).client_setname, name, None, name)

    def cluster(self, cluster_arg, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.cluster',
                super(OcRedis, self).cluster, cluster_arg, None, cluster_arg, *args)

    def config_get(self, *args, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.config_get',
                super(OcRedis, self).config_get, None, None, *args, **kwargs)

    def config_resetstat(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.config_resetstat',
                super(OcRedis, self).config_resetstats, None, None)

    def config_rewrite(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.config_rewrite',
                super(OcRedis, self).config_rewrite, None, None)

    def config_set(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.config_set',
                super(OcRedis, self).config_set, name, value)

    def dbsize(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.dbsize',
                super(OcRedis, self).dbsize, None, None)

    def dbsize(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.dbsize',
                super(OcRedis, self).dbsize, None, None)

    def debug_object(self, key):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.debug_object',
                super(OcRedis, self).debug_object, key, None)

    def decr(self, name, amount=1):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.decr',
                super(OcRedis, self).decr, name, None, name, amount)

    def decrby(self, name, amount=1):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.decrby',
                super(OcRedis, self).decrby, name, None, name, amount)

    def delete(self, *names):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.delete',
                super(OcRedis, self).delete, names, None, *names)

    def dump(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.dump',
                super(OcRedis, self).dump, name, None)

    def echo(self, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.echo',
                super(OcRedis, self).echo, value, None)

    def eval(self, script, numkeys, *keys_and_args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.eval',
                super(OcRedis, self).eval, script, None, script, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.evalsha',
                super(OcRedis, self).evalsha, None, None, sha, numkeys, *keys_and_args)

    def exists(self, *names):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.exxists',
                super(OcRedis, self).exists, names, None, *names)

    def expire(self, name, time):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.expire',
                super(OcRedis, self).expire, name, time, name, time)

    def flushall(self, asynchronous=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.flushall',
                super(OcRedis, self).flushall, None, None, asynchronous)

    def geoadd(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.geoadd',
                super(OcRedis, self).geoadd, name, None, name, *values)

    def geodist(self, name, place1, place2, unit=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.geodist',
                super(OcRedis, self).geodist, name, None, name, place1, place2, unit)

    def geohash(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.geohash',
                super(OcRedis, self).geohash, name, values, name, *values)

    def geopos(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.geopos',
                super(OcRedis, self).geopos, name, values, name, *values)

    def georadius(self, name, longitude, latitude, radius, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.georadius',
                super(OcRedis, self).georadius, name, None, name, longitude, latitude, **kwargs)

    def georadiusbymember(self, name, member, radius, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.georadiusbymember',
                super(OcRedis, self).georadiusbymember, name, member, name, member, raidus, **kwargs)

    def get(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.get',
                super(OcRedis, self).get, name, None, name)

    def getbit(self, name, offset):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.getbit',
                super(OcRedis, self).getbit, name, None, name, offset)

    def getrange(self, key, start, end):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.getrange',
                super(OcRedis, self).getrange, key, None, key, start, end)

    def getset(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.getset',
                super(OcRedis, self).getset, name, value, name, value)

    def hdel(self, name, *keys):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hdel',
                super(OcRedis, self).hdel, name, keys, name, *keys)

    def hexists(self, name, key):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hexists',
                super(OcRedis, self).hexists, name, key, name, key)

    def hget(self, name, key):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hget',
                super(OcRedis, self).hget, name, key, name, key)

    def hgetall(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hgetall',
                super(OcRedis, self).hgetall, name, None, name)

    def hincrby(self, name, key, amount=1):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hincrby',
                super(OcRedis, self).hincrby, name, key, name, key, amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hincrbyfloat',
                super(OcRedis, self).hincrbyfloat, name, key, name, key, amount)

    def hkeys(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hkeys',
                super(OcRedis, self).hkeys, name, None, name)

    def hlen(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hlen',
                super(OcRedis, self).hlen, name, None, name)

    def hmget(self, name, key, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hmget',
                super(OcRedis, self).hmget, name, key, name, key, *args)

    def hmset(self, name, mapping):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hmset',
                super(OcRedis, self).hmset, name, mapping, name, mapping)

    def hscan(self, name, cursor=0, match=None, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hscan',
                super(OcRedis, self).hscan, name, None, name, cursor, match, count)

    def hscan_iter(self, name, match=None, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hscan_iter',
                super(OcRedis, self).hscan_iter, name, None, name, match, count)

    def hset(self, name, key, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hset',
                super(OcRedis, self).hset, key, value, name, key, value)

    def hsetnx(self, name, key, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hsetnx',
                super(OcRedis, self).hsetnx, key, value, name, key, value)

    def hstrlen(self, name, key):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hstrlen',
                super(OcRedis, self).hstrlen, name, key, name, key)

    def hvals(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.hvals',
                super(OcRedis, self).hvals, name, None, name)

    def incr(self, name, amount=1):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.incr',
                super(OcRedis, self).incr, name, None, name, amount)

    def incrby(self, name, amount=1):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.incrby',
                super(OcRedis, self).incrby, name, None, name, amount)

    def incrbyfloat(self, name, amount=1.0):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.incrbyfloat',
                super(OcRedis, self).incrbyfloat, name, None, name, amount)

    def info(self, section=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.info',
                super(OcRedis, self).info, None, None, section)

    def keys(self, pattern=u'*'):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.keys',
                super(OcRedis, self).keys, pattern, None, pattern)

    def lastsave(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lastsave',
                super(OcRedis, self).lastsave, None, None)

    def lindex(self, name, index):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lindex',
                super(OcRedis, self).lindex, name, None, name, index)

    def linsert(self, name, where, refvalue, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.linsert',
                super(OcRedis, self).linsert, name, value, name, where, refvalue, value)

    def llen(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.llen',
                super(OcRedis, self).llen, name, None, name)

    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None, lock_class=None, thread_local=True):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lock',
                super(OcRedis, self).lock, name, None, name, timeout, sleep, blocking_timeout, thread_local)

    def lpop(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lpop',
                super(OcRedis, self).lpop, name, None, name)

    def lpush(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lpush',
                super(OcRedis, self).lpop, name, values, name, values)

    def lpushx(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lpushx',
                super(OcRedis, self).lpushx, name, value, name, value)

    def lrange(self, name, start, end):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lrange',
                super(OcRedis, self).lrange, name, None, name, start, end)

    def lrem(self, name, start, end):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lrem',
                super(OcRedis, self).lrem, name, None, name, start, end)

    def lset(self, name, index, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.lset',
                super(OcRedis, self).lset, name, value, name, index, value)

    def ltrim(self, name, start, end):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.ltrim',
                super(OcRedis, self).ltrim, name, None, name, start, end)

    def memory_purge(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.memory_purge',
                super(OcRedis, self).memory_purge, None, None)

    def memory_usage(self, key, samples=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.memory_usage',
                super(OcRedis, self).memory_usage, key, None, key, samples)

    def mget(self, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.mget',
                super(OcRedis, self).mget, keys, args, keys, *args)

    def migrate(self, host, port, keys, destination_db, timeout, copy=False, replace=False, auth=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.migrate',
                super(OcRedis, self).migrate, keys, None, host, port, keys, destination_db, timeout, copy, replace, auth)

    def move(self, name, db):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.move',
                super(OcRedis, self).move, name, None, name, db)

    def mset(self, mapping):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.mset',
                super(OcRedis, self).mset, mapping, None, mapping)

    def msetnx(self, mapping):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.msetnx',
                super(OcRedis, self).msetnx, mapping, None, mapping)

    def object(self, infotype, key):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.object',
                super(OcRedis, self).object, infotype, key, infotype, key)

    def persist(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.persist',
                super(OcRedis, self).persist, name, None, name)

    def pexpire(self, name, time):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pexpire',
                super(OcRedis, self).pexpire, name, None, name, time)

    def pfadd(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pfadd',
                super(OcRedis, self).pfadd, name, values, name, *values)

    def ping(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.ping',
                super(OcRedis, self).ping, None, None)

    def pipeline(self, transaction=True, shard_hint=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pipeline',
                super(OcRedis, self).pipeline, None, None, transaction, shard_hint)

    def psetex(self, name, time_ms, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.psetex',
                super(OcRedis, self).psetex, name, value, name, time_ms, value)

    def pttl(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pttl',
                super(OcRedis, self).pttl, name, None)

    def publish(self, channel, message):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.publish',
                super(OcRedis, self).publish, channel, message, channel, message)

    def pubsub(self, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pubsub',
                super(OcRedis, self).pubsub, None, None, **kwargs)

    def pubsub_channels(self, pattern=u'*'):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pubsub_channels',
                super(OcRedis, self).pubbsub_channels, pattern, None, pattern)

    def pubsub_numpat(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pubsub_numpat',
                super(OcRedis, self).pubsub_numpat, None, None)

    def pubsub_numsub(self, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.pubsub_numsub',
                super(OcRedis, self).pubsub_numsub, None, None, *args)

    def randomkey(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.randomkey',
                super(OcRedis, self).randomkey, None, None)

    def register_script(self, script):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.register_script',
                super(OcRedis, self).register_script, script, None, script)

    def rename(self, src, dst):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.rename',
                super(OcRedis, self).rename, src, dst, src, dst)

    def renamenx(self, src, dst):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.renamenx',
                super(OcRedis, self).renamenx, src, dst, src, dst)

    def restore(self, name, ttl, value, replace=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.restore',
                super(OcRedis, self).restore, name, value, name, ttl, value, replace)

    def rpop(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.rpop',
                super(OcRedis, self).rpop, name, None, name)

    def rpoplpush(self, src, dst):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.rpoplpush',
                super(OcRedis, self).rpop, src, dst, src, dst)

    def rpush(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.rpush',
                super(OcRedis, self).rpush, name, None, name)

    def rpushx(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.rpushx',
                super(OcRedis, self).rpush, name, value, name, value)

    def sadd(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sadd',
                super(OcRedis, self).sadd, name, values, name, *values)

    def save(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.save',
                super(OcRedis, self).save, None, None)

    def scan(self, cursor=0, match=None, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.scan',
                super(OcRedis, self).scan, None, None, cursor, match, count)

    def scan_iter(self, match=None, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.scan_iter',
                super(OcRedis, self).scan_iter, None, None, match, count)

    def scard(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.scard',
                super(OcRedis, self).scard, name, None, name)

    def script_exists(self, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.script_exists',
                super(OcRedis, self).script_exists, args, None, *args)

    def script_flush(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.script_flush',
                super(OcRedis, self).script_flush, None, None)

    def script_kill(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.script_kill',
                super(OcRedis, self).script_kill, None, None)

    def script_load(self, script):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.script_load',
                super(OcRedis, self).script_load, script, None, script)

    def sdiff(self, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sdiff',
                super(OcRedis, self).sdiff, keys, None, keys, *args)

    def sdiffstore(self, dest, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sdiffstore',
                super(OcRedis, self).sdiffstore, keys, None, dest, keys, *args)

    def sentinel(self, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel',
                super(OcRedis, self).sentinel, None, None, *args)

    def sentinel_get_master_addr_by_name(self, service_name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_get_master_addr_by_name',
                super(OcRedis, self).sentinel_get_master_addr_by_name, service_name, None, service_name)

    def sentinel_master(self, service_name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_master',
                super(OcRedis, self).sentinel_master, service_name, None, service_name)

    def sentinel_masters(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_masters',
                super(OcRedis, self).sentinel_masters, None, None)

    def sentinel_monitor(self, name, ip, port, quorum):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_monitor',
                super(OcRedis, self).sentinel_monitor, name, None, name, ip, port, quorum)

    def sentinel_remove(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_remove',
                super(OcRedis, self).sentinel_remove, name, None, name)

    def sentinel_sentinels(self, service_name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_sentinels',
                super(OcRedis, self).sentinel_sentinels, service_name, None, service_name)

    def sentinel_set(self, name, option, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_set',
                super(OcRedis, self).sentinel_set, name, value, name, option, value)

    def sentinel_slaves(self, service_name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sentinel_slaves',
                super(OcRedis, self).sentinel_slaves, service_name, None, service_name)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.set',
                super(OcRedis, self).set, name, value, name, value, ex, px, nx, xx)

    def set_response_callback(self, command, callback):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.set_response_callback',
                super(OcRedis, self).set_response_callback, command, None, command, callback)

    def setbit(self, name, offset, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.setbit',
                super(OcRedis, self).setbit, name, value, name, offset, value)

    def setex(self, name, time, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.setex',
                super(OcRedis, self).setex, name, value, name, time, value)

    def setnx(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.setnx',
                super(OcRedis, self).setnx, name, value, name, value)

    def setrange(self, name, offset, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.setrange',
                super(OcRedis, self).setrange, name, value, name, offset, value)

    def shutdown(self, save=False, nosave=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.shutdown',
                super(OcRedis, self).shutdown, None, None, save, nosave)

    def sinter(self, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sinter',
                super(OcRedis, self).sinter, keys, args, keys, *args)

    def sinterstore(self, dest, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sinterstore',
                super(OcRedis, self).sinterstore, keys, dest, dest, keys, *args)

    def sismember(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sismember',
                super(OcRedis, self).sismember, name, value, name, value)

    def slaveof(self, host=None, port=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.slaveof',
                super(OcRedis, self).slaveof, host, port, host, port)

    def slowlog_get(self, num=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.slowlog_get',
                super(OcRedis, self).slowlog_get, None, None, num)

    def slowlog_len(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.slowlog_len',
                super(OcRedis, self).slowlog_len, None, None)

    def slowlog_reset(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.slowlog_reset',
                super(OcRedis, self).slowlog_reset, None, None)

    def smembers(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.smembers',
                super(OcRedis, self).smembers, name, None, name)

    def smove(self, src, dst, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.smove',
                super(OcRedis, self).smove, src, value, src, dst, value)

    def sort(self, name, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None, groups=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sort',
                super(OcRedis, self).sort, name, None, name, start, num, by, get, desc, alpha, store, groups)

    def spop(self, name, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.spop',
                super(OcRedis, self).spop, name, None, name, count)

    def srandmember(self, name, number=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.srandmember',
                super(OcRedis, self).srandmember, name, None, name, number)

    def srem(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.srem',
                super(OcRedis, self).srem, name, values, name, *values)

    def sscan(self, name, cursor=0, match=None, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sscan',
                super(OcRedis, self).sscan, name, match, name, cursor, match, count)

    def sscan_iter(self, name, match=None, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sscan_iter',
                super(OcRedis, self).sscan_iter, name, None, name, match, count)

    def strlen(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.strlen',
                super(OcRedis, self).strlen, name, None, name)

    def substr(self, name, start, end=-1):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.substr',
                super(OcRedis, self).substr, name, None, name, start, end)

    def sunion(self, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sunion',
                super(OcRedis, self).sunion, keys, None, keys, *args)

    def sunionstore(self, dest, keys, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.sunionstore',
                super(OcRedis, self).sunionstore, dest, keys, dest, keys, *args)

    def swapdb(self, first, second):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.swapdb',
                super(OcRedis, self).swapdb, first, second, first, second)

    def time(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.time',
                super(OcRedis, self).swapdb, None, None)

    def touch(self, *args):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.touch',
                super(OcRedis, self).touch, args, None, *args)

    def transaction(self, func, *watches, **kwargs):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.transaction',
                super(OcRedis, self).transaction, None, None, *watches, **kwargs)

    def ttl(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.ttl',
                super(OcRedis, self).ttl, name, None, name)

    def type(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.type',
                super(OcRedis, self).type, name, None, name)

    def unlink(self, *names):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.unlink',
                super(OcRedis, self).unlink, names, None, names)

    def unwatch(self):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.unwatch',
                super(OcRedis, self).unwatch, None, None)

    def wait(self, num_replicas, timeout):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.wait',
                super(OcRedis, self).wait, None, None, num_replicas, timeout)

    def watch(self, *names):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.watch',
                super(OcRedis, self).watch, names, None, *names)

    def xack(self, name, groupname, *ids):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xack',
                super(OcRedis, self).xack, name, groupname, name, groupname, *ids)

    def xadd(self, name, fields, id=u'*', maxlen=None, approximate=True):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xadd',
                super(OcRedis, self).xadd, name, fields, name, fields, id, maxlen, approximate)

    def xclaim(self, name, groupname, consumername, min_idle_time, message_ids, idle=None, time=None,
                    retryCount=None, force=False, justid=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xclaim',
                super(OcRedis, self).xclaim, name, groupname, name, groupname, consumername, min_idle_time, message_ids,
                idle, time, retryCount, force, justid)

    def xdel(self, name, *ids):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xdel',
                super(OcRedis, self).xdel, name, ids, name, *ids)

    def xgroup_create(self, name, groupname, id=u'$', mkstream=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xgroup_create',
                super(OcRedis, self).xgroup_create, name, groupname, name, groupname, id, mkstream)

    def xgroup_delconsumer(self, name, groupname, consumername):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xgroup_delconsumer',
                super(OcRedis, self).xgroup_delconsumer, name, groupname, name, groupname, consumername)

    def xgroup_destroy(self, name, groupname):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xgroup_destroy',
                super(OcRedis, self).xgroup_destroy, name, groupname, name, groupname)

    def xgroup_setid(self, name, groupname, id):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xgroup_setid',
                super(OcRedis, self).xgroup_setid, name, groupname, name, groupname, id)

    def xinfo_consumers(self, name, groupname):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xgroup_consumers',
                super(OcRedis, self).xgroup_consumers, name, groupname, name, groupname)

    def xinfo_groups(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xinfo_groups',
                super(OcRedis, self).xinfo_groups, name, None, name)

    def xinfo_stream(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xinfo_stream',
                super(OcRedis, self).xinfo_stream, name, None, name)

    def xlen(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xlen',
                super(OcRedis, self).xlen, name, None, name)

    def xpending(self, name, groupname):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xpending',
                super(OcRedis, self).xpending, name, groupname, name, groupname)

    def xpending_range(self, name, groupname, min, max, count, consumername=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xpending_range',
                super(OcRedis, self).xpending_range, name, groupname, name, groupname, min, max, count, consumername)

    def xrange(self, name, min=u'-', max=u'+', count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xrange',
                super(OcRedis, self).xrange, name, None, name, min, max, count)

    def xread(self, streams, count=None, block=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xread',
                super(OcRedis, self).xread, streams, None, streams, count, block)

    def xreadgroup(self, groupname, consumername, streams, count=None, block=None, noack=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xreadgroup',
                super(OcRedis, self).xreadgroup, groupname, consumername, groupname, consumername, streams, count, block, noack)

    def xrevrange(self, name, max=u'+', min=u'-', count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xrevrange',
                super(OcRedis, self).xrevrange, name, None, name, max, min, count)

    def xtrim(self, name, maxlen, approximate=True):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.xtrim',
                super(OcRedis, self).xtrim, name, None, name, maxlen, approximate)

    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zadd',
                super(OcRedis, self).zadd, name, mapping, name, mapping, nx, xx, ch, incr)

    def zcard(self, name):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zcard',
                super(OcRedis, self).zcard, name, None, name)

    def zcount(self, name, min, max):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zcount',
                super(OcRedis, self).zcount, name, None, name, min, max)

    def zincrby(self, name, amount, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zincrby',
                super(OcRedis, self).zincrby, name, None, name, amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zinterstore',
                super(OcRedis, self).zinterstore, dest, keys, dest, keys, aggregate)

    def zlexcount(self, name, min, max):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zlexcount',
                super(OcRedis, self).zlexcount, name, None, name, min, max)

    def zpopmax(self, name, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zpopmax',
                super(OcRedis, self).zpopmax, name, None, name, count)

    def zpopmin(self, name, count=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zpopmin',
                super(OcRedis, self).zpopmin, name, None, name, count)

    def zrange(self, name, start, end, desc=False, withscores=False, score_cast_func=float):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrange',
                super(OcRedis, self).zrange, name, None, name, start, end, desc, withscores, score_cast_func)

    def zrangebylex(self, name, min, max, start=None, num=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrangebylex',
                super(OcRedis, self).zrangebylex, name, None, name, min, max, start, num)

    def zrangebyscore(self, name, min, max, start=None, num=None, withscores=False, score_cast_func=float):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrangebyscore',
                super(OcRedis, self).zrangebyscore, name, None, name, min, max, start, num, withscores, score_cast_func)

    def zrank(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrank',
                super(OcRedis, self).zrank, name, value, name, value)

    def zrem(self, name, *values):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrem',
                super(OcRedis, self).zrem, name, values, name, *values)

    def zremrangebylex(self, name, min, max):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zremrangebylex',
                super(OcRedis, self).zremrangebylex, name, None, name, min, max)

    def zremrangebyrank(self, name, min, max):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zremrangebyrank',
                super(OcRedis, self).zremrangebyrank, name, None, name, min, max)

    def zremrangebyscore(self, name, min, max):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zremrangebyscore',
                super(OcRedis, self).zremrangebyscore, name, None, name, min, max)

    def zrevrange(self, name, start, end, withscores=False, score_cast_func=float):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrevrange',
                super(OcRedis, self).zrevrange, name, None, name, start, end, withscores, score_cast_func)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrevrangebylex',
                super(OcRedis, self).zrevrangebylex, name, None, name, max, min, start, num)

    def zrevrangebyscore(self, name, start, end, withscores=False, score_cast_func=float):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrevrangebyscore',
                super(OcRedis, self).zrevrangebyscore, name, None, name, start, end, withscores, score_cast_func)

    def zrevrank(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zrevrank',
                super(OcRedis, self).zrevrank, name, value, name, value)

    def zscan(self, name, cursor=0, match=None, count=None, score_cast_func=float):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zscan',
                super(OcRedis, self).zscan, name, cursor, name, cursor, match, count, score_cast_func)

    def zscan_iter(self, name, match=None, count=None, score_cast_func=float):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zscan_iter',
                super(OcRedis, self).zscan_iter, name, None, name, match, count, score_cast_func)

    def zscore(self, name, value):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zscore',
                super(OcRedis, self).zscore, name, value, name, value)

    def zunionstore(self, dest, keys, aggregate=None):
        return trace_and_record_stats_with_key_and_value(
                'redispy.Redis.zunionstore',
                super(OcRedis, self).zunionstore, dest, keys, dest, keys, aggregate)
