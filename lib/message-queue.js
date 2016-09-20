var _ = require('lodash');
var Q = require('q');

// TODO... what indexes should be created and should this be responsible for creating them?

function MessageQueue() {
  var self = this;

  self.errorHandler = console.error;

  self.databasePromise = null;
  self.collectionName = '_queue';

  self.pollingInterval = 1000;
  self.processingTimeout = 30 * 1000;
  self.maxWorkers = 5;

  self._workers = {};
  self._numWorkers = 0;
  self._pollingIntervalId = null;

  self.registerWorker = function(type, promise) {
    self._workers[type] = promise;
    self._startPolling();
  };

  self.stopPolling = function() {
    self._stopPolling();
  };

  self.enqueue = function(type, message, options) {
    var queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message
    };
    if (options && options.nextReceivableTime) {
      queueItem.nextReceivableTime = options.nextReceivableTime;
    }
    return self._enqueue(queueItem);
  };

  self.enqueueAndProcess = function(type, message) {
    var queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message,
      receivedTime: new Date()
    };
    return self._enqueue(queueItem)
      .then(function(queueItem) {
        return self._process(queueItem);
      });
  };

  //region Private Helper Methods

  self._startPolling = function() {
    if (!self._pollingIntervalId) {
      // Try and find work at least once every pollingInterval
      self._pollingIntervalId = setInterval(self._poll, self.pollingInterval);
    }
  };

  self._stopPolling = function() {
    if (self._pollingIntervalId) {
      clearInterval(self._pollingIntervalId);
    }
  };

  self._poll = function() {
    if (self._numWorkers < self.maxWorkers) {
      return self._receive()
        .then(function(queueItem) {
          if (queueItem) {
            return self._process(queueItem)
              .then(function() {
                // Look for more work to do immediately if we just processed something
                setImmediate(self._poll);
              });
          }
        })
        .catch(self.errorHandler);
    }
  };

  self._process = function(queueItem) {
    var worker = self._workers[queueItem.type];
    if (!worker) { return Q.reject(new Error("No worker registered for type: " + queueItem.type)); }

    self._numWorkers++;
    return worker(queueItem)
      .then(function(status) {
        self._numWorkers--;
        switch (status) {
          case "Completed":
            return self._dequeue(queueItem);
          case "Retry":
            return self._release(queueItem);
          case "Rejected":
            return self._reject(queueItem);
          default:
            throw new Error("Unknown status: " + status);
        }
      });
  };

  self._enqueue = function(queueItem) {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).insertOne(queueItem);
      })
      .then(function(result) {
        return result.ops[0];
      });
  };

  self._dequeue = function(queueItem) {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).deleteOne({_id: queueItem._id});
      })
      .then(function(result) {
        return result.deletedCount;
      });
  };

  self._release = function(queueItem) {
    var update = {
      $unset: {
        receivedTime: ""
      },
      $set: {
        retryCount: queueItem.retryCount ? queueItem.retryCount + 1 : 1,
        nextReceivableTime: queueItem.nextReceivableTime ? queueItem.nextReceivableTime : new Date()
      },
      $push: {
        releaseHistory: {
          retryCount: queueItem.retryCount ? queueItem.retryCount : 0,
          receivedTime: queueItem.receivedTime,
          releasedTime: new Date(),
          releasedReason: queueItem.releasedReason
        }
      }
    };

    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).updateOne({_id: queueItem._id}, update);
      })
      .then(function(result) {
        return result.modifiedCount;
      });
  };

  self._reject = function(queueItem) {
    var update = {
      $unset: {
        receivedTime: "",
        nextReceivableTime: ""
      },
      $set: {
        rejectedTime: new Date(),
        rejectionReason: queueItem.rejectionReason
      },
      $push: {
        releaseHistory: {
          retryCount: queueItem.retryCount ? queueItem.retryCount : 0,
          receivedTime: queueItem.receivedTime,
          releasedTime: new Date(),
          releasedReason: queueItem.releasedReason
        }
      }
    };

    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).updateOne({_id: queueItem._id}, update);
      })
      .then(function(result) {
        return result.modifiedCount;
      });
  };

  self._receive = function() {
    var query = {
      type: {$in: _.keys(self._workers)},
      rejectedTime: {$exists: false},
      $and: [
        {
          $or: [
            {nextReceivableTime: {$lt: new Date()}},
            {nextReceivableTime: {$exists: false}}
          ]
        },
        {
          $or: [
            {receivedTime: {$lt: new Date(Date.now() - self.processingTimeout)}},
            {receivedTime: {$exists: false}}
          ]
        }
      ]
    };
    var update = {
      $set: {
        receivedTime: new Date()
      }
    };

    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).findOneAndUpdate(query, update, {returnOriginal: false});
      })
      .then(function(result) {
        return result.value;
      });
  };

  //endregion
}

module.exports = MessageQueue;
