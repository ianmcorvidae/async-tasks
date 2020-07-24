async-tasks
-----------

This service tracks and manages asynchronous tasks throughout the DE backend services.

API
===

Available endpoints:

 - `GET /`: basic status-check endpoint
 - `GET /debug/vars`: standard golang expvar-provided endpoint
 - `GET /tasks/:id`: list an async task by ID
 - `DELETE /tasks/:id`: delete a task
 - `POST /tasks/:id/status`: update the status of a task
 - `POST /tasks/:id/behaviors`: add a behavior to a task
 - `GET /tasks`: get many tasks using a provided filter
 - `POST /tasks`: create a new task

The data structures that the POST endpoints expect are best learned by looking at the `model` package, except for the available filters, which are easiest found in the definition of `GetByFilterRequest`, the implementation of that endpoint.
