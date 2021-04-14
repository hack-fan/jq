# Redis job queue
A simple job queue use redis as backend.

It just uses Redis List, not Redis Stream.

Use this for:

* The simplest multi-publisher multi-worker model.
* Random small processing delays are acceptable.
* Want to know what it does.

Not for:

* Blocked consumers
* FanOut model
* Strict Message Time Series
