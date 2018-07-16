"""
Workflow: Ordering a coffee in a shop.
--------------------------------------

This is a workflow when someone is entering in a shop, ordering a coffee and
a chocolate chip cookie, pays the bill and tips, get the order and leave the
coffee shop.

Workflow:
    1. Enter in the coffee shop.
    2. Order.
        2.1 Coffee.
        2.2 Chocolate chip cookie.
    3. Finalize the order (any of the activites below can be done in any order)
        3.1 Pays.
        3.2 Get the order.
    4. Leave the coffee shop.

Result:
    entering coffee shop
    ordering: coffee
    ordering: chocolate_chip_cookie
    pay $6.98
    get order
    leaving the coffee shop
"""

from garcon import activity
from garcon import runner
from garcon import task


class Workflow:

    def __init__(self, client, domain, name):
        """Create the workflow.

        Args:
            domain (str): the domain to attach this workflow to.
            name (str): the name of the workflow.
        """
        self.name = name
        self.domain = domain
        self.client = client
        self.create_activity = activity.create(client, domain, name)

    def decider(self, schedule, context=None):
        """Custom deciders.

        The custom decider is a method that allows you to write more complex
        workflows based on some input and output. In our case, we have a
        workflow that triggers n steps based on a value passed in the context
        and triggers a specific behavior if another value is present.

        Args:
            schedule (callable): method that is used to schedule a specific
                activity.
            context (dict): the current context.
        """

        # The schedule method takes an "activity id" (it's not the name of
        # the activity, it's a unique id that defines the activity you
        # are trying to schedule, it's your responsibility to make it unique).
        # The second argument is the reference to the ActivityWorker.
        enter = schedule('enter', self.create_enter_coffee_activity)
        enter.wait()

        total = 0
        for item in ['coffee', 'chocolate_chip_cookie']:
            activity_name = 'order_{item}'.format(item=item)
            activity = schedule(activity_name,
                self.create_order_activity,
                input={'item': item})

            # Getting the result of the previous activity to calculate the
            # total the user will be charged.
            total += activity.result.get('price')

        # The `input` param is the data that will be sent to the activity.
        pay_activity = schedule(
            'pay', self.create_payment_activity,
            input={'total': total})

        get_order = schedule('get_order', self.create_get_order_activity)
        pay_activity.wait(), get_order.wait()

        # Leaving the coffee shop will not happen before the payment has
        # been processed and the order has been taken.
        schedule('leave_coffee_shop', self.create_leave_coffee_shop)

    @property
    def create_enter_coffee_activity(self):
        """Create the activity when user enters coffee shop."""
        return self.create_activity(
            name='enter',
            run=runner.Sync(
                lambda context, activity:
                    print('entering coffee shop')))

    @property
    def create_order_activity(self):
        """Create an order for an item.

        Returns:
            ActivityWorker: the activity that create an order item.
        """
        @task.decorate()
        def order(activity, item=None):
            print('ordering: {}'.format(item))
            price = 0.00
            if item == 'coffee':
                price = 3.99
            if item == 'chocolate_chip_cookie':
                price = 2.99
            return {'price': price}

        return self.create_activity(
            name='order',
            run=runner.Sync(order.fill(item='item')))

    @property
    def create_payment_activity(self):
        """Pay the bill.

        Returns:
            ActivityWorker: the activity that pays the bills.
        """
        @task.decorate()
        def payment(activity, total=None):
            print('pay ${}'.format(total))

        return self.create_activity(
            name='payment',
            run=runner.Sync(payment.fill(total='total')))

    @property
    def create_get_order_activity(self):
        """Get order.

        Returns:
            ActivityWorker: the activity that gets the order.
        """
        @task.decorate()
        def payment(activity):
            print('get order')

        return self.create_activity(
            name='get_order',
            run=runner.Sync(payment.fill()))

    @property
    def create_leave_coffee_shop(self):
        """Leave the coffee shop.

        Returns:
            ActivityWorker: the activity that leaves the coffee shop.
        """
        @task.decorate()
        def leave(activity):
            print('Leaving the coffee shop')

        return self.create_activity(
            name='leave',
            run=runner.Sync(leave.fill()))
