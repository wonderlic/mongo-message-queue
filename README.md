# mongo-message-queue

[![NPM version](https://badge.fury.io/js/mongo-message-queue.svg)](http://badge.fury.io/js/mongo-message-queue)

### A promise based message queue for Node.js using a mongodb backing store.

## Package Dependency Notice

NOTE: This package requires mongodb version 6.x.x or higher.

## Usage

### Installation

```
npm install mongo-message-queue --save
```

### Require & Instantiate

Example:

```javascript
const MessageQueue = require('mongo-message-queue');

const mQueue = new MessageQueue();
```

### Database Configuration

Set the .databasePromise property to a function that returns a promise that (eventually) returns a database connection.

Example:

```javascript
const MongoClient = require('mongodb').MongoClient;

const mongoUri = ...;
const mongoOptions = ...;

mQueue.databasePromise = function() {
  // Return a promise to return a mongo database connection here...
  return MongoClient.connect(mongoUri, mongoOptions);
};
```

### Register one or more Workers

Use the .registerWorker method to provide a processing method for a specific type of message in the queue.

Each registered worker will handle a specific type of item in the queue as specified by the first parameter. Registering an additional worker for the same type will override the processing method that is used for that type.

The second parameter of the .registerWorker method is used to specify the processing method for work of that type. When work shows up in the message queue, the provided processing method will be called and provided the queueItem as the first parameter.

- Use queueItem.message to get access to the enqueued item.
- Use queueItem.retryCount to get access to the number of retries that have occurred.
- This property is not available during the first processing attempt.

The processing method should return a promise (chain) that (eventually) returns a status of "Completed", "Rejected", or "Retry".

- If the returned status is "Rejected"...
- Optionally set queueItem.rejectionReason to indicate why this should no longer be processed.
- Optionally set queueItem.releasedReason to indicate why this didn't process this time around.
- If the returned status is "Retry"...
- Optionally set queueItem.releasedReason to indicate why this didn't process this time around.
- Optionally set queueItem.nextReceivableTime to indicate when you want this to get picked up and re-processed.
  - If you don't set queueItem.nextReceivableTime this message will be available for re-procesing immediately.

Registering a worker will cause the message queue to immediately start polling for work of the specified type. By default, the message queue looks for new messages at least once every second (configurable by overridding the .pollingInterval property). Polling also occurs immediately following the processing of a previous queue message as long as there is still available work in the queue. Polling will continue to occur until the .stopPolling() method is called.

Example:

```javascript
mQueue.registerWorker('doSomething', function (queueItem) {
  // Return a promise to do something here...
  return database
    .collection('somecollection')
    .updateOne({_id: queueItem.message.id}, {status: queueItem.message.status})
    .then(function (result) {
      return 'Completed';
    })
    .catch(function (err) {
      queueItem.releasedReason = err.message;
      if ((queueItem.retryCount || 0) < 5) {
        queueItem.nextReceivableTime = new Date(Date.now() + 30 * 1000); // Retry after 30 seconds...
        return 'Retry';
      } else {
        queueItem.rejectionReason = 'Gave up after 5 retries.';
        return 'Rejected';
      }
    });
});
```

### Send work to the message queue

Enqueue a message to eventually be picked up and processed by one of the workers.

Example:

```javascript
mQueue.enqueue('doSomething', {id: 123, status: 'done'});
```

You can also enqueue a message to be picked up in the future (instead of being immediately available for pickup) by passing in an optional options object with a future nextReceivableTime as the third parameter.

Example:

```javascript
mQueue.enqueue('doSomething', {id: 123, status: 'done'}, {nextReceivableTime: new Date(Date.now() + 30 * 1000)});
```

You can also enqueue a message and try to process it immediately with a locally registered worker. If there is not a worker for the specified type registered locally, it will get picked up and processed later when there is an available worker.

Example:

```javascript
mQueue.enqueueAndProcess('doSomething', {id: 123, status: 'done'});
```

## License

(The MIT License)

Copyright (c) 2014-2023 Wonderlic, Inc. <SoftwareDevelopment@wonderlic.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
