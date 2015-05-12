Code Sample
===========

Before going onto the details, let’s take a quick look at the Garcon’s
implementation of `Serial Activity Execution <http://docs.pythonboto.org/en/latest/swf_tut.html#serial-activity-execution>`_:

.. code-block:: python

    from garcon import activity
    from garcon import runners


    domain = 'dev'
    name = 'boto_tutorial'
    create = activity.create(domain, name)

    a_tasks = create(
        name='a_tasks',
        run=runner.Sync(
            lambda context, activity: dict(result='Now don’t be givin him sambuca!'))

    b_tasks = create(
        name='b_tasks',
        requires=[a_tasks],
        run=runner.Sync(
            lambda context, activity: print(context)))

    c_tasks = create(
        name='c_tasks',
        requires=[b_tasks],
        run=runner.Sync(
            lambda context, activity: print(context)))

By way of comparison, check out the `implementation <https://gist.github.com/xethorn/62695a072bb4f15726fd>`_
using directly boto.

Note:
    Notes: Executing this code shows that the activity “a_tasks” returns a
    dictionary which hydrates the execution context. When the activity “b_tasks”
    is executed, the context passed for its execution contains the key/value
    previously passed as an output. Same observation can be done in “c_tasks”.

All activities are running in series. `More examples <https://github.com/xethorn/garcon/tree/master/example>`_
(including runners) are available online.
