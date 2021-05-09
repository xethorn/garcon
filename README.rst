|BuildStatus| |Downloads| |CoverageStatus|

Lightweight library for AWS SWF.

    Garcon deals with easy going clients and kitchens. It takes orders
    from clients (deciders), and send them to the kitchen (activities).
    Difficult clients and kitchens can be handled directly by the
    restaurant manager.

Requirements
~~~~~~~~~~~~

-  Python 3.5, 3.6, 3.7, 3.8 (tested)
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

The code sample shows a workflow where a user enters a coffee shop, orders
a coffee and a chocolate chip cookie. All ordered items are prepared and
completed, the user pays the order, receives the ordered items, then leave
the shop.

The code below represents the workflow decider. For the full code sample,
see the `example`_.

.. code:: python

    enter = schedule('enter', self.create_enter_coffee_activity)
    enter.wait()

    total = 0
    for item in ['coffee', 'chocolate_chip_cookie']:
        activity_name = 'order_{item}'.format(item=item)
        activity = schedule(activity_name,
            self.create_order_activity,
            input={'item': item})
        total += activity.result.get('price')
        
    pay_activity = schedule(
        'pay', self.create_payment_activity,
        input={'total': total})

    get_order = schedule('get_order', self.create_get_order_activity)
    
    # Waiting for paying and getting the order to complete before
    # we let the user leave the coffee shop.
    pay_activity.wait(), get_order.wait()
    schedule('leave_coffee_shop', self.create_leave_coffee_shop)


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

Trusted by
~~~~~~~~~~

|The Orchard| |Sony Music| |DataArt|

Contributors
~~~~~~~~~~~~

-  Michael Ortali (Author)
-  Adam Griffiths
-  Raphael Antonmattei
-  John Penner

.. _xethorn: github.com/xethorn
.. _rantonmattei: github.com/rantonmattei
.. _someboredkiddo: github.com/someboredkiddo
.. _example: https://github.com/xethorn/garcon/tree/master/example/custom_decider

.. |BuildStatus| image:: https://github.com/xethorn/garcon/workflows/Build/badge.svg
   :target: https://github.com/xethorn/garcon/actions?query=workflow%3ABuild+branch%3Amaster

.. |Downloads| image:: https://img.shields.io/pypi/dm/garcon.svg
   :target: https://coveralls.io/r/xethorn/garcon?branch=master

.. |CoverageStatus| image:: https://coveralls.io/repos/xethorn/garcon/badge.svg?branch=master
   :target: https://coveralls.io/r/xethorn/garcon?branch=master
   
.. |The Orchard| image:: https://media-exp1.licdn.com/dms/image/C4E0BAQGi7o5g9l4JWg/company-logo_200_200/0/1519855981606?e=2159024400&v=beta&t=WBe-gOK2b30vUTGKbA025i9NFVDyOrS4Fotx9fMEZWo
    :target: https://theorchard.com

.. |Sony Music| image:: https://media-exp1.licdn.com/dms/image/C4D0BAQE9rvU-3ig-jg/company-logo_200_200/0/1604099587507?e=2159024400&v=beta&t=eAAubphf_fI-5GEb0ak1QnmtRHmc8466Qj4sGrCsWYc
    :target: https://www.sonymusic.com/
    
.. |DataArt| image:: https://media-exp1.licdn.com/dms/image/C4E0BAQGRi6OIlNQG8Q/company-logo_200_200/0/1519856519357?e=2159024400&v=beta&t=oi6HQpzoeTKA082s-8Ft75vGTvAkEp4VHRyMLeOHXoo
    :target: https://www.dataart.com/
