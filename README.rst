ocredis
=======

ocredis is a wrapper for the popular [redis-py](https://github.com/andymccurdy/redis-py)

ocredis provides observability using OpenCensus for distributed tracing and metrics.

.. image:: https://badge.fury.io/py/ocredis.svg
       :target: https://pypi.org/project/ocredis/


Installing it
-------------

.. code-block:: bash

    pip install ocredis


Using it
--------

You can initialize exactly how you would for redis.Redis.
In fact it is meant to be a drop replacement.

* Change the import statement from

.. code-block:: pycon

    >>> import redis
    
to

.. code-block:: pycon

    >>> import ocredis
    
* Change the client initialization from
  

.. code-block:: pycon

  >>> client = redis.Redis(host=host, port=port)
  
to

.. code-block:: pycon

  >>> client = ocredis.OcRedis(host=host, port=port)`

and obviously enabling OpenCensus metrics and exporters as per https://opencensus.io/exporters/supported-exporters/python/

.. code-block:: pycon

  >>> ocredis.register_views()

and the rest is trivial to use then.

For example

.. code-block:: pycon

  >>> import ocredis
  >>> ocredis.register_views()
  >>> r = ocredis.OcRedis(host='localhost', port=6379)
  >>> r.set('foo', 'bar') 
  True
  >>> r.get('foo')
  'bar'

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

Tests
-----
Tests can be run by using pytest, for example

.. code-block:: bash

    pytest
