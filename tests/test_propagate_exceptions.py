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

import pytest

from opencensus.trace import execution_context
from opencensus.trace.samplers import always_on
from opencensus.trace.tracer import Tracer
from opencensus.stats import stats
from opencensus.stats.aggregation import CountAggregation, DistributionAggregation

import ocredis

class RetainerTraceExporter(object):
    def __init__(self):
        self.__spans = []

    def export(self, span_data_list):
        self.__spans.extend(span_data_list)

    def emit(self, span_data_list):
        self.__spans.extend(span_data_list)

    def spans(self):
        return self.__spans


class RetainerStatsExporter(object):
    def __init__(self):
        self.__view_data = []
        self.__views     = []

    def export(self, view_data_list):
        self.__view_data.extend(view_data_list)

    def on_register_view(self, view):
        self.__views.append(view)

    def view_data(self):
        return self.__view_data


def test_ensure_exceptions_are_raised_yet_reported():
    span_retainer = RetainerTraceExporter()
    tracer = Tracer(sampler=always_on.AlwaysOnSampler(), exporter=span_retainer)
    execution_context.set_opencensus_tracer(tracer)

    view_data_retainer = RetainerStatsExporter()
    view_manager = stats.Stats().view_manager
    view_manager.register_exporter(view_data_retainer)
    ocredis.register_views()

    with pytest.raises(Exception):
        invalid_port = 1<<18
        client = ocredis.OcRedis(host='localhost', port=invalid_port)
        client.get('newer')

    spans = span_retainer.spans()
    assert len(spans) == 1

    span0 = spans[0]
    assert span0.name == 'redispy.Redis.get'

    # Ensure that the span for .get is the root span.
    assert span0.parent_span_id == None

    # Now check that the top most span has a Status
    root_span_status = span0.status
    assert root_span_status.code == 2 # Unknown as per https://opencensus.io/tracing/span/status/#status-code-mapping
    assert root_span_status.message == 'Error 8 connecting to localhost:262144. nodename nor servname provided, or not known.'
    assert root_span_status.details == None

    # Next let's check that stats are recorded.
    view_data_list = view_data_retainer.view_data()
    assert len(view_data_list) >= 2

    # Expecting the values for the various views per method.
    # However, since stats recording is time-imprecise we can
    # less or more values recorded, hence bucketize view_data by
    # name first and then perform the various assertions.
    view_data_by_name = bucketize_view_data_by_name(view_data_list)

    calls_view_data_list = view_data_by_name['redispy/calls']
    assert len(calls_view_data_list) > 0

    latency_view_data_list = view_data_by_name['redispy/latency']
    assert len(latency_view_data_list) > 0

    calls_view_data_get = calls_view_data_list[0]
    latency_view_data_execute_command = latency_view_data_list[0]

    count_aggregation = CountAggregation()
    view_calls_execute_command = calls_view_data_get.view
    assert view_calls_execute_command.aggregation.aggregation_type == count_aggregation.aggregation_type
    # assert view_calls_execute_command.aggregation.count == 1
    assert view_calls_execute_command.name == "redispy/calls"
    assert view_calls_execute_command.description == "The number of calls"
    assert view_calls_execute_command.columns == ['method', 'error', 'status']
    # calls_execute_command_tag_values = view_calls_execute_command.get_tag_values(
    #                    calls_view_data_get.columns,  view_calls_execute_command.columns)

    calls_tag_values = calls_view_data_get.tag_value_aggregation_data_map.keys()
    sorted_calls_tag_values = sorted(calls_tag_values, key=lambda tag_value_tuple: tag_value_tuple[0])
    print(sorted_calls_tag_values)
    assert len(sorted_calls_tag_values) >= 1
    assert sorted_calls_tag_values[0] == (
                'redispy.Redis.get',
                'Error 8 connecting to localhost:262144. nodename nor servname provided, or not known.',
                'ERROR',
            )
    

    latency_distribution_aggregation = DistributionAggregation()
    view_latency_execute_command = latency_view_data_execute_command.view
    assert view_latency_execute_command.aggregation.aggregation_type == latency_distribution_aggregation.aggregation_type
    # assert view_calls_execute_command.aggregation.count == 1
    assert view_latency_execute_command.name == "redispy/latency"
    assert view_latency_execute_command.description == "The distribution of the latencies per method"
    assert view_latency_execute_command.columns == ['method', 'error', 'status']
    # calls_execute_command_tag_values = view_calls_execute_command.get_tag_values(
    #                    calls_view_data_get.columns,  view_calls_execute_command.columns)

    # TODO: File a bug with OpenCensus-Python about them using strings
    # for start and endtime, instead of actual date* objects on which we
    # can easily calculate time spent etc.
    assert latency_view_data_execute_command.start_time != ''
    assert latency_view_data_execute_command.end_time != ''
    # latency_ms = latency_view_data_execute_command.end_time - latency_view_data_execute_command.start_time
    # assert latency_ms > 0.0

def bucketize_view_data_by_name(view_data_list):
    by_name_buckets = {}
    for view_data in view_data_list:
        by_name_buckets.setdefault(view_data.view.name, []).append(view_data)
    return by_name_buckets
