|Build Status| |Coverage Status|

Lightweight library for AWS SWF.

    Garcon deals with easy going clients and kitchens. It takes orders
    from clients (deciders), and send them to the kitchen (activities).
    Difficult clients and kitchens can be handled directly by the
    restaurant manager.

Requirements
~~~~~~~~~~~~

-  Python 2.7, 3.4, 3.5, 3.6, 3.7 (tested)
-  Boto3 (tested)

Goal
~~~~

The goal of this library is to allow the creation of Amazon Simple
Workflow without the need to worry about the orchestration of the
different activities and building out the different workers. This
framework aims to help simple workflows. If you have a more complex
case, you might want to use directly boto.

Code sample
~~~~~~~~~~~

The code sample shows a workflow that has 4 activities. It starts with
activity\_1, which after being completed schedule activity\_2 and
activity\_3 to be ran in parallel. The workflow ends after the
completion of activity\_4 which requires activity\_2 and activity\_3 to
be completed.

.. code:: python

    from __future__ import print_function

    import boto3
    from garcon import activity
    from garcon import runner

    client = boto3.client('swf', region_name='us-east-1')

    domain = 'dev'
    name = 'workflow_sample'
    create = activity.create(client, domain, name)

    test_activity_1 = create(
        name='activity_1',
        run=runner.Sync(
            lambda activity, context: print('activity_1')))

    test_activity_2 = create(
        name='activity_2',
        requires=[test_activity_1],
        run=runner.Async(
            lambda activity, context: print('activity_2_task_1'),
            lambda activity, context: print('activity_2_task_2')))

    test_activity_3 = create(
        name='activity_3',
        requires=[test_activity_1],
        run=runner.Sync(
            lambda activity, context: print('activity_3')))

    test_activity_4 = create(
        name='activity_4',
        requires=[test_activity_3, test_activity_2],
        run=runner.Sync(
            lambda activity, context: print('activity_4')))

Application architecture
~~~~~~~~~~~~~~~~~~~~~~~~

::

    .
    ├── cli.py # Instantiate the workers
    ├── flows # All your application flows.
    │   ├── __init__.py
    │   └── example.py # Should contain a structure similar to the code sample.
    ├── tasks # All your tasks
    │   ├── __init__.py
    │   └── s3.py # Task that focuses on s3 files.
    └── task_example.py # Your different tasks.

Contributors
~~~~~~~~~~~~

-  Michael Ortali
-  Adam Griffiths
-  Raphael Antonmattei
-  John Penner

.. _xethorn: github.com/xethorn
.. _rantonmattei: github.com/rantonmattei
.. _someboredkiddo: github.com/someboredkiddo

.. |Build Status| image:: https://travis-ci.org/xethorn/garcon.svg
   :target: https://travis-ci.org/xethorn/garcon
.. |Coverage Status| image:: https://coveralls.io/repos/xethorn/garcon/badge.svg?branch=master
   :target: https://coveralls.io/r/xethorn/garcon?branch=master
