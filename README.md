garcon [![Build Status](https://travis-ci.org/xethorn/garcon.svg?branch=master)](https://travis-ci.org/xethorn/garcon) ![Downloads](https://pypip.in/d/garcon/badge.png) [![Coverage Status](https://coveralls.io/repos/xethorn/garcon/badge.svg?branch=master)](https://coveralls.io/r/xethorn/garcon?branch=master)
======

Lightweight library for AWS SWF.

> Garcon deals with easy going clients and kitchens. It takes orders
> from clients (deciders), and send them to the kitchen (activities). Difficult
> clients and kitchens can be handled directly by the restaurant manager.

Requirements
------------

* Python 2.7, 3.4 (tested)
* Boto 2.34.0 (tested)

Goal
----

The goal of this library is to allow the creation of Amazon Simple Workflow
without the need to worry about the orchestration of the different activities
and building out the different workers. This framework aims to help simple
workflows. If you have a more complex case, you might want to use directly
boto.

Code sample
-----------

The code sample shows a workflow that has 4 activities. It starts with
activity_1, which after being completed schedule activity_2 and activity_3 to
be ran in parallel. The workflow ends after the completion of activity_4 which
requires activity_2 and activity_3 to be completed.

```python
from __future__ import print_function
import boto.swf.layer2 as swf

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
```

Features
--------

Garcon  provides several useful features that will enable you to perform more
complex actions:


- Timeouts: most timeout are calculated automatically.
- Generators: They spawn one or more instances of an activity based on values
  provided in the context
  ([see example](https://gist.github.com/xethorn/2cefcc85d5093b12d065).)
- Task lists: You can programatically write a task list.
  ([see example](https://gist.github.com/mortaliorchard/6eca8a1723eea16ff2ac))


Application architecture
------------------------

```
.
├── cli.py # Instantiate the workers
├── flows # All your application flows.
│   ├── __init__.py
│   └── example.py # Should contain a structure similar to the code sample.
├── tasks # All your tasks
│   ├── __init__.py
│   └── s3.py # Task that focuses on s3 files.
└── task_example.py # Your different tasks.
```

Contributors
------------

* Michael Ortali (@[xethorn](github.com/xethorn))
* Adam Griffiths ([@adamlwgriffiths](github.com/adamlwgriffiths))
* Raphael Antonmattei (@[rantonmattei](github.com/rantonmattei))
