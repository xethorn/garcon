Garcon
======

Lightweight library for AWS SWF.

Requirements
------------

* Python 3.8, 3.9, 3.10, 3.11, 3.12 (tested)
* Boto3 1.38.21 (tested)


Goal
----

The goal of this library is to allow the creation of Amazon Simple Workflow without the need to worry about the orchestration of the different activities and building out the different workers. This framework aims to help simple workflows. If you have a more complex case, you might want to use directly Boto3.

Code sample
-----------

The code sample shows a workflow where a user enters a coffee shop, orders
a coffee and a chocolate chip cookie. All ordered items are prepared and
completed, the user pays the order, receives the ordered items, then leave
the shop.

The code below represents the workflow decider. For the full code sample,
see the `example`_ directory in the repository.

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

.. _example: https://github.com/xethorn/garcon/tree/master/example/custom_decider


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
