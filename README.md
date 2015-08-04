# mongo-message-queue

[![NPM version](https://badge.fury.io/js/mongo-message-queue.svg)](http://badge.fury.io/js/mongo-message-queue)

### A promise based message queue for Node.js using a mongo backing store.

## Installation

```
npm install mongo-message-queue --save
```

## Package Dependencies

NOTE:  This package makes use of promises (instead of callbacks) when interacting with mongodb which requires mongodb version 2.0.36 or higher.

## Usage

```javascript
var MessageQueue = require('mongo-message-queue');

var mQueue = new MessageQueue();
...

// Provide a promise to return a database connection.
mQueue.databasePromise = function() {
  // Return a promise to return a mongo database connection here...
};

...

// Register a worker that can handle items of a specific type in the queue.
// This will cause the message queue to start polling for work of the specified type.
mQueue.registerWorker('doSomething', function(queueItem) {
  // Return a promise to do something here...
  
  // Use queueItem.message to get access to the enqueued item.
  // Use queueItem.retryCount to get access to the number of retries that have occurred.
  //    This property is not available if this is the first iteration.

  // The final return value in the promise chain should be a status string of:
  //   "Completed", "Rejected", or "Retry"
  // If the returned status is "Rejected"...
  //    Optionally set queueItem.rejectionReason to indicate why this should no longer be processed.
  //    Optionally set queueItem.releasedReason to indicate why this didn't process this time around.
  // If the returned status is "Retry"...
  //    Optionally set queueItem.releasedReason to indicate why this didn't process this time around.
  //    Optionally set queueItem.nextReceivableTime to indicate when you want this to get picked up and re-processed.
  //      If you don't set queueItem.nextReceivableTime this will be available for re-procesing immediately.

  });

...

// Enqueue a message to eventually be picked up by one of the workers.
// It will get picked up when there is an available worker.
mQueue.enqueue('doSomething', { some: 'message' });

// Enqueue a message and try to process it immediately with a locally registered worker.
// If the specified type is not registered locally, it will get picked up when there is an available worker.
mQueue.enqueueAndProcess('doSomething', { some: 'message' });
```

## License

(The MIT License)

Copyright (c) 2014 Wonderlic, Inc. <SoftwareDevelopment@wonderlic.com>

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
