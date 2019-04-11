var _ = require('lodash');
var Q = require('q');

function MessageQueue() {
  var self = this;

  self.errorHandler = console.error;

  self.databasePromise = null;
  self.collectionName = '_queue';

  self.pollingInterval = 1000;
  self.processingTimeout = 30 * 1000;
  self.maxWorkers = 5;

  var _workers = {};
  var _numWorkers = 0;
  var _pollingIntervalId = null;

  self.registerWorker = function(type, promise) {
    _workers[type] = promise;
    _startPolling();
  };

  self.stopPolling = function() {
    _stopPolling();
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

    // The priority range is 1-10, 1 being the highest.
    queueItem.priority = _.get(options, 'priority', 1);

    return _enqueue(queueItem);
  };

  self.enqueueAndProcess = function(type, message) {
    var queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message,
      receivedTime: new Date()
    };
    return _enqueue(queueItem)
      .then(function() {
        return _process(queueItem);
      });
  };

  self.remove = function(type, messageQuery) {
    var query = _buildQueueItemQuery(type, messageQuery);

    return _removeByQuery(query);
  };

  self.update = function(type, messageQuery, messageUpdate, nextReceivableTime) {
    var query = _buildQueueItemQuery(type, messageQuery);
    var update = _buildQueueItemUpdate(messageUpdate, nextReceivableTime);

    return _updateByQuery(query, update);
  };

  self.ensureIndexes = function(keyPatterns, options) {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        var ensureIndexPromises = [];
        keyPatterns.forEach(function(keyPattern) {
          ensureIndexPromises.push(collection(self.collectionName).ensureIndex(keyPattern, options));
        });
        return Q.all(ensureIndexPromises);
      })
  };

  //region Private Helper Methods

  function _startPolling() {
    if (!_pollingIntervalId) {
      // Try and find work at least once every pollingInterval
      _pollingIntervalId = setInterval(_poll, self.pollingInterval);
    }
  }

  function _stopPolling() {
    if (_pollingIntervalId) {
      clearInterval(_pollingIntervalId);
    }
  }

  function _poll() {
    if (_numWorkers < self.maxWorkers) {
      _numWorkers++;
      _receive()
        .then(function(queueItem) {
          if (queueItem) {
            return _process(queueItem)
              .then(function() {
                // Look for more work to do immediately if we just processed something
                setImmediate(_poll);
              });
          }
        })
        .catch(self.errorHandler)
        .finally(function() {
          _numWorkers--;
        });
    }
  }

  function _process(queueItem) {
    var worker = _workers[queueItem.type];
    if (!worker) { return Q.reject(new Error("No worker registered for type: " + queueItem.type)); }

    return worker(queueItem)
      .then(function(status) {
        switch (status) {
          case "Completed":
            return _dequeue(queueItem);
          case "Retry":
            return _release(queueItem);
          case "Rejected":
            return _reject(queueItem);
          default:
            throw new Error("Unknown status: " + status);
        }
      });
  }

  function _enqueue(queueItem) {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).insertOne(queueItem);
      })
      .then(function(result) {
        return result.ops[0];
      });
  }

  function _dequeue(queueItem) {
    return _removeByQuery({_id: queueItem._id});
  }

  function _removeByQuery(query) {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).deleteOne(query);
      })
      .then(function(result) {
        return result.deletedCount;
      });
  }

  function _updateByQuery(query, update) {
    if (!self.databasePromise) { return Q.reject(new Error("No database configured")); }
    return self.databasePromise()
      .then(function(db) {
        return db.collection(self.collectionName).updateOne(query, update)
      })
      .then(function(result) {
        return result.modifiedCount;
      });
  }

  function _release(queueItem) {
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

    return _updateByQuery({_id: queueItem._id}, update);
  }

  function _reject(queueItem) {
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
  }

  function _receive() {
    var query = {
      type: {$in: _.keys(_workers)},
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
        return db.collection(self.collectionName).findOneAndUpdate(query, update, {returnOriginal: false, sort: 'priority'});
      })
      .then(function(result) {
        return result.value;
      });
  }

  function _buildQueueItemQuery(type, messageQuery) {
    var query = {type: type};
    
    _.forEach(messageQuery, function(value, key) {
      var property = 'message.' + key;
      query[property] = value;
    });

    return query;
  }

  function _buildQueueItemUpdate(messageUpdate, nextReceivableTime) {
    var update = {
      $set: {
        nextReceivableTime: nextReceivableTime,
      }
    };
    
    _.forEach(messageUpdate, function(value, key) {
      var property = 'message.' + key;
      update.$set[property] = value;
    });

    return update;
  }

  //endregion
}

module.exports = MessageQueue;
