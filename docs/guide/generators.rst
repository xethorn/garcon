Generators
==========

Generators spawn one or more instances of an activity based on values provided
in the context.

One of our use case includes a job that calls an API each day to get metrics
for all the countries in the world. If the API fails for one country, the entire
activity fails — retrying it means we will have to restart the entire list of
countries.

Instead of having one activity to do all calls, it’s a lot more robust to have
one activity per country and have a retry mechanism applied to it. Failures
will only be contained for one country that has failed instead of all.

Note:
    Be aware that SWF has a limit on the number of events the history can hold,
    always make sure the number of activities spawned by the generator will
    allow enough room.

Example:

.. code-block:: python

    from garcon import activity
    from garcon import runner
    from garcon import task
    import random


    domain = 'dev'
    name = 'country_flow'
    create = activity.create(domain, name)


    def country_generator(context):
        # We limit this so you can more easily see the failures / retries.
        total_countries = 6
        for country_id in range(1, total_countries):
            yield {'generator.country_id': country_id}


    @task.decorate()
    def unstable_country_task(activity, country_id):
        num = int(random.random() * 4)
        base = 'activity_2_country_id_{country_id}'.format(
            country_id=country_id)

        if num == 3:
            # Make the task randomly fail.
            print(base, 'has failed')
            raise Exception('fails')

        print(base, 'has succeeded')


    test_activity_1 = create(
        name='activity_1',
        tasks=runner.Sync(
            lambda context, activity: print('activity_1')))

    test_activity_2 = create(
        name='activity_2',
        requires=[test_activity_1],
        generators=[
            country_generator],
        retry=3,
        tasks=runner.Sync(
            unstable_country_task.fill(country_id='generator.country_id')))

    test_activity_3 = create(
        name='activity_3',
        requires=[test_activity_2],
        tasks=runner.Sync(
            lambda context, activity: print('end of flow')))

Note:
    Generators attribute takes a list of generators. If you have a flow that
    has a date range, list of countries, you can create activities that
    corresponds to one day and one specific countries. If you have 10 days in
    your range and 20 countries, you will run 200 activities.
