ocredis
======

Redis-Py wrapper that provides observability using OpenCensus for
distributed tracing and metrics.


Tests
-----
Tests can be run by using `pytest`, for example
```shell
pytest
```


Metrics available
-----------------

- calls
- latency
- key_length
- value_length

.. csv-table::
    :header: "Metric", "View Name", "Unit", "Tags"
    :widths: 20, 20, 20, 20

    "Latency", "redispy/latency", "ms", "'error', 'method', 'status'"
    "Calls", "redispy/calls", "1", "'error', 'method', 'status'"
    "Key lengths", "redispy/key_length", "By", "'error', 'method', 'status'"
    "Value lengths", "redispy/value_length", "By", "'error', 'method', 'status'"


Installing it
-------------

pip install ocredis


Using it
--------

You can initialize exactly how you would for redis.Redis. In fact it is meant to be a drop replacement with just:

- Changing the import statement from "import redis" to "import ocredis"
- Changing the client initialization from "client = redis.Redis(host=host, port=port)" to "client = ocredis.OcRedis(host=host, port=port)"

and the rest is trivial to use then.
