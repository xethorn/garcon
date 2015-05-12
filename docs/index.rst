Garcon
======

Lightweight library for AWS SWF.

Requirements
------------

* Python 2.7, 3.4 (tested)
* Boto 2.34.0 (tested)


Goal
----

The goal of this library is to allow the creation of Amazon Simple Workflow without the need to worry about the orchestration of the different activities and building out the different workers. This framework aims to help simple workflows. If you have a more complex case, you might want to use directly boto.

Code sample
-----------

The code sample shows a workflow that has 4 activities. It starts with activity_1, which after being completed schedule activity_2 and activity_3 to be ran in parallel. The workflow ends after the completion of activity_4 which requires activity_2 and activity_3 to be completed::

  from __future__ import print_function

  from garcon import activity
  from garcon import runner


  domain = 'dev'
  create = activity.create(domain)

  test_activity_1 = create(
      name='activity_1',
      tasks=runner.Sync(
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

Documentation
-------------

.. toctree::
    :titlesonly:

    guide
    api
    releases


Licence
-------

This web site and all documentation is licensed under `Creative
Commons 3.0 <http://creativecommons.org/licenses/by/3.0/>`_.
