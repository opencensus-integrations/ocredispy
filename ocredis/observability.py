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

import time

from opencensus.trace import execution_context
from opencensus.trace.status import Status
from opencensus.trace.tracer import noop_tracer

from opencensus.stats import stats
from opencensus.stats import aggregation
from opencensus.stats import measure
from opencensus.stats import view
from opencensus.tags import tag_key
from opencensus.tags import tag_map
from opencensus.tags import tag_value

key_error = tag_key.TagKey("error")
key_method = tag_key.TagKey("method")
key_status = tag_key.TagKey("status")

m_latency_ms = measure.MeasureFloat("redispy/latency", "The latency per call in milliseconds", "ms")
m_key_length = measure.MeasureInt("redispy/key_length", "The length of each key", "By")
m_value_length = measure.MeasureInt("redispy/value_length", "The length of each value", "By")


def register_views():
    all_tag_keys = [key_method, key_error, key_status]
    calls_view = view.View("redispy/calls", "The number of calls",
            all_tag_keys,
            m_latency_ms,
            aggregation.CountAggregation())

    latency_view = view.View("redispy/latency", "The distribution of the latencies per method", 
            all_tag_keys,
            m_latency_ms,
            aggregation.DistributionAggregation([
            # Latency in buckets:
            # [
            #    >=0ms, >=5ms, >=10ms, >=25ms, >=40ms, >=50ms, >=75ms, >=100ms, >=200ms, >=400ms,
            #    >=600ms, >=800ms, >=1s, >=2s, >=4s, >=6s, >=10s, >-20s, >=50s, >=100s
            # ]
                0, 5, 10, 25, 40, 50, 75, 1e2, 2e2, 4e2,
                6e2, 8e2, 1e3, 2e3, 4e3, 6e3, 1e4, 2e4, 5e4, 10e5
            ])
    )
  
    key_lengths_view = view.View("redispy/key_lengths", "The distribution of the key lengths",
            all_tag_keys,
            m_key_length,
            aggregation.DistributionAggregation([
            # Key length buckets:
            # [
            #   0B, 5B, 10B, 20B, 50B, 100B, 200B, 500B, 1000B, 2000B, 5000B
            # ]
                0, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000
            ])
    )

    value_lengths_view = view.View("redispy/value_lengths", "The distribution of the value lengths",
            all_tag_keys,
            m_value_length,
            aggregation.DistributionAggregation([
            # Value length buckets:
            # [
            #   0B, 5B, 10B, 20B, 50B, 100B, 200B, 500B, 1000B, 2000B, 5000B, 10000B, 20000B
            # ]
                0, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000
            ])
    )
    view_manager = stats.Stats().view_manager
    for each_view in [calls_view, latency_view, key_lengths_view, value_lengths_view]:
        view_manager.register_view(each_view)


## Some extracted types for comparisons inside function "lengths"
strType = type('')
tupleType = type((1,2,))
listType = type([])
dictType = type(dict())


def heuristical_lengths(items):
    """
    heuristical_lengths tries to deriver the lengths of the content of items.
    It always returns a list.
    a) If typeof(items) is a string, it'll return [len(items)]
    b) If typeof(items) is a dict, it'll return [len(items)]
    c) If typeof(items) is either list or tuple, it'll best case try to iterate
       over each element and record those lengths and return them all flattened.
       If it can't retrieve the lengths yet len(items) > 0, then it will return [len(items)]
    d) If items has the '__len__' attribute, it'll return [len(items)]
    e) Otherwise if it can't derive the type, it'll return []
    """
    typ = type(items)

    if isinstance(typ, strType):
        return [len(items)]

    elif isinstance(typ, dictType):
        return [len(items)]

    elif isinstance(typ, tupleType) or isinstance(typ, listType):
        lengths = []
        for item in items:
            i_lengths = lengths(item)
            lengths.extend(i_lengths)

        # In the best case, if len(lengths) == 0
        # yet len(items) > 0, just use len(items)
        if len(lengths) == 0 and len(items) > 0:
            lengths = [len(items)]

        return lengths

    elif hasattr(items, '__len__'):
        return [len(items)]

    elif hasattr(items, '__iter__'):
        lengths = []
        itr = iter(items)
        for it in itr:
            it_lengths = heuristical_lengths(it)
            lengths.extend(it_lengths)

        return lengths

    else:
       return []


def trace_and_record_stats_with_key_and_value(method_name, fn, key, value, *args, **kwargs):
    __TRACER = execution_context.get_opencensus_tracer() or noop_tracer.NoopTracer()
    __STATS_RECORDER = stats.Stats().stats_recorder

    start_time = time.time()
    tags = tag_map.TagMap()
    tags.insert(key_method, tag_value.TagValue(method_name))
    mm = __STATS_RECORDER.new_measurement_map()

    with __TRACER.span(name=method_name) as span:
        try:
            return fn(*args, **kwargs)

        except Exception as e:
            span.status = Status.from_exception(e)
            tags.insert(key_status, "ERROR")
            tags.insert(key_error, e.__str__())
            # Re-raise that exception after we've extracted the error.
            raise e

        else:
            tags.insert(key_status, "OK")

        finally:
            latency_ms = (time.time() - start_time) * 1e3
            mm.measure_float_put(m_latency_ms, latency_ms)
            key_lengths = heuristical_lengths(key)
            value_lengths =  heuristical_lengths(value)

            for key_length in key_lengths:
                mm.measure_int_put(m_key_length, key_length)

            for value_length in value_lengths:
                mm.measure_int_put(m_value_length, value_length)

            mm.record(tags)
