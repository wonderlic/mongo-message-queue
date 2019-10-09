var _ = require('lodash');

function MessageQueue() {
  var self = this;

  self.errorHandler = console.error;

  self.databasePromise = null;
  self.collectionName = '_queue';
  self.createIndex = false;

  self.pollingInterval = 1000;
  self.processingTimeout = 30 * 1000;
  self.maxWorkers = 5;

  var _workers = {};
  var _numWorkers = 0;
  var _pollingIntervalId = null;

  self.registerWorker = function(type, promise) {
    _workers[type] = promise;
    return _startPolling();
  };

  self.stopPolling = function() {
    _stopPolling();
  };

  self.enqueue = function(type, message, options = {}) {
    var queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message
    };

    if (options.nextReceivableTime) {
      queueItem.nextReceivableTime = options.nextReceivableTime;
    }

    // The priority range is 1-10, 1 being the highest.
    queueItem.priority = _.get(options, 'priority', 1);

    return _enqueue(queueItem);
  };

  self.enqueueAndProcess = async function(type, message) {
    var queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message,
      receivedTime: new Date()
    };

    await _enqueue(queueItem);

    return _process(queueItem);
  };

  self.removeOne = function(type, messageQuery) {
    var query = _buildQueueItemQuery(type, messageQuery);

    return _remove(query);
  };

  self.removeMany = function(type, messageQuery) {
    var query = _buildQueueItemQuery(type, messageQuery);

    return _remove(query, true);
  };

  self.updateOne = function(type, messageQuery, messageUpdate, options) {
    var query = _buildQueueItemQuery(type, messageQuery);
    var update = _buildQueueItemUpdate(messageUpdate, options);

    return _update(query, update);
  };

  self.updateMany = function(type, messageQuery, messageUpdate, options) {
    var query = _buildQueueItemQuery(type, messageQuery);
    var update = _buildQueueItemUpdate(messageUpdate, options);

    return _update(query, update, true);
  };

  //region Private Helper Methods

  async function _startPolling() {
    if (!_pollingIntervalId) {
      // Temporarily assigning polling interval to prevent multiple polls starting while index is being created
      _pollingIntervalId = -1;

      if (self.createIndex) {
        await _createQueueIndex();
      }

      // Try and find work at least once every pollingInterval
      _pollingIntervalId = setInterval(_poll, self.pollingInterval);
    }
  }

  async function _createQueueIndex() {
    const db = await self.databasePromise();
    await db.collection(self.collectionName).createIndex({
      type: 1,
      rejectedTime: 1,
      nextReceivableTime: 1,
      receivedTime: 1
    });
  }

  function _stopPolling() {
    if (_pollingIntervalId) {
      clearInterval(_pollingIntervalId);
    }
  }

  async function _poll() {
    if (_numWorkers < self.maxWorkers) {
      _numWorkers += 1;

      try {
        const queueItem = await _receive();

        if (queueItem) {
          await _process(queueItem);

          // Look for more work to do immediately if we just processed something
          setImmediate(_poll);
        }
      } catch (err) {
        self.errorHandler(err);
      } finally {
        _numWorkers -= 1;
      }
    }
  }

  async function _process(queueItem) {
    var worker = _workers[queueItem.type];
    if (!worker) {
      throw new Error(`No worker registered for type: ${queueItem.type}`);
    }

    const status = await worker(queueItem);
    switch (status) {
      case 'Completed':
        return _removeOneById(queueItem._id);
      case 'Retry':
        return _release(queueItem);
      case 'Rejected':
        return _reject(queueItem);
      default:
        throw new Error(`Unknown status: ${status}`);
    }
  }

  async function _enqueue(queueItem) {
    if (!self.databasePromise) {
      throw new Error('No database configured');
    }

    const db = await self.databasePromise();
    const result = await db.collection(self.collectionName).insertOne(queueItem);
    return result.ops[0];
  }

  function _removeOneById(id) {
    return _remove({ _id: id });
  }

  async function _remove(query, multi) {
    if (!self.databasePromise) {
      throw new Error('No database configured');
    }

    const db = await self.databasePromise();

    let result;
    if (multi) {
      result = await db.collection(self.collectionName).deleteOne(query);
    } else {
      result = await db.collection(self.collectionName).deleteMany(query);
    }
    return result.deletedCount;
  }

  function _updateOneById(id, update) {
    return _update({ _id: id }, update);
  }

  async function _update(query, update, multi) {
    if (!self.databasePromise) {
      throw new Error('No database configured');
    }

    const db = await self.databasePromise();

    let result;
    if (multi) {
      result = await db.collection(self.collectionName).updateOne(query, update);
    } else {
      result = await db.collection(self.collectionName).updateMany(query, update);
    }
    return result.modifiedCount;
  }

  function _release(queueItem) {
    var update = {
      $unset: {
        receivedTime: ''
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

    return _updateOneById(queueItem._id, update);
  }

  async function _reject(queueItem) {
    var update = {
      $unset: {
        receivedTime: '',
        nextReceivableTime: ''
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

    if (!self.databasePromise) {
      throw new Error('No database configured');
    }

    const db = await self.databasePromise();
    const result = await db.collection(self.collectionName).updateOne({ _id: queueItem._id }, update);
    return result.modifiedCount;
  }

  async function _receive() {
    var query = {
      type: { $in: _.keys(_workers) },
      rejectedTime: { $exists: false },
      $and: [
        {
          $or: [{ nextReceivableTime: { $lt: new Date() } }, { nextReceivableTime: { $exists: false } }]
        },
        {
          $or: [
            { receivedTime: { $lt: new Date(Date.now() - self.processingTimeout) } },
            { receivedTime: { $exists: false } }
          ]
        }
      ]
    };
    var update = {
      $set: {
        receivedTime: new Date()
      }
    };

    if (!self.databasePromise) {
      throw new Error('No database configured');
    }

    const db = await self.databasePromise();

    const result = await db
      .collection(self.collectionName)
      .findOneAndUpdate(query, update, { returnOriginal: false, sort: 'priority' });
    return result.value;
  }

  function _buildQueueItemQuery(type, messageQuery) {
    var query = { type };

    _.forEach(messageQuery, function(value, key) {
      var property = `message.${key}`;
      query[property] = value;
    });

    return query;
  }

  function _buildQueueItemUpdate(messageUpdate, options) {
    var update = {};
    var $set = {};

    if (options.nextReceivableTime) {
      $set.nextReceivableTime = options.nextReceivableTime;
    }

    _.forEach(messageUpdate, function(value, key) {
      var property = `message.${key}`;
      $set[property] = value;
    });

    if (!_.isEmpty($set)) {
      update.$set = $set;
    }

    return update;
  }

  //endregion
}

module.exports = MessageQueue;
