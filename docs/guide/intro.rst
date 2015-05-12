Introduction
============

Garcon is a Python library for Amazon SWF, originally built at
`The Orchard <http://www.theorchard.com/about-us/jobs/>`_.

The goal of this library is to allow the creation of workflows using SWF
without the need to worry about the orchestration of the different activities,
and build out the complex different workers.

Main Features:

* Simple: when you write a flow, the deciders and the activity workers are
  automatically generated. No extra work is required.
* Retry mechanisms: if an activity has failed, you can set a maximum of retries.
  It ends up very useful when you work with external APIs.
* Scalable timeouts: all the timeout are calculated and consider other running
  workflows.
* :doc:`Activity Generators </guide/generators>`: some workflows requires more
  than one instance of a specific activity.
