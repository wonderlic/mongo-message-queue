const _ = require('lodash');

// TODO... what indexes should be created and should this be responsible for creating them?

function MessageQueue() {
  const self = this;

  self.errorHandler = console.error;

  self.databasePromise = null;
  self.collectionName = '_queue';

  self.pollingInterval = 1000;
  self.processingTimeout = 30 * 1000;
  self.maxWorkers = 5;

  const _workers = {};
  let _numWorkers = 0;
  let _pollingIntervalId = null;

  self.registerWorker = function (type, promise) {
    _workers[type] = promise;
    _startPolling();
  };

  self.stopPolling = function () {
    _stopPolling();
  };

  self.enqueue = function (type, message, options = {}) {
    const queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message,
    };

    if (options.nextReceivableTime) {
      queueItem.nextReceivableTime = options.nextReceivableTime;
    }

    // The priority range is 1-10, 1 being the highest.
    queueItem.priority = _.get(options, 'priority', 1);

    return _enqueue(queueItem);
  };

  self.enqueueAndProcess = async function (type, message) {
    const queueItem = {
      dateCreated: new Date(),
      type: type,
      message: message,
      receivedTime: new Date(),
    };

    await _enqueue(queueItem);
    return _process(queueItem);
  };

  self.removeOne = function (type, messageQuery) {
    const query = _buildQueueItemQuery(type, messageQuery);
    return _removeOne(query);
  };

  self.removeMany = function (type, messageQuery) {
    const query = _buildQueueItemQuery(type, messageQuery);
    return _removeMany(query);
  };

  self.updateOne = function (type, messageQuery, messageUpdate, options) {
    const query = _buildQueueItemQuery(type, messageQuery);
    const update = _buildQueueItemUpdate(messageUpdate, options);
    return _updateOne(query, update);
  };

  self.updateMany = function (type, messageQuery, messageUpdate, options) {

    const query = _buildQueueItemQuery(type, messageQuery);
    const update = _buildQueueItemUpdate(messageUpdate, options);
    return _updateMany(query, update);
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
    const worker = _workers[queueItem.type];
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
    const collection = await _getCollection();
    const result = await collection.insertOne(queueItem);

    if (result.ops !== undefined) {
      return result.ops[0];
    }

    return result;
  }

  function _release(queueItem) {
    const update = {
      $unset: {
        receivedTime: '',
      },
      $set: {
        retryCount: queueItem.retryCount ? queueItem.retryCount + 1 : 1,
        nextReceivableTime: queueItem.nextReceivableTime ? queueItem.nextReceivableTime : new Date(),
      },
      $push: {
        releaseHistory: {
          retryCount: queueItem.retryCount ? queueItem.retryCount : 0,
          receivedTime: queueItem.receivedTime,
          releasedTime: new Date(),
          releasedReason: queueItem.releasedReason,
        },
      },
    };

    return _updateOneById(queueItem._id, update);
  }

  function _reject(queueItem) {
    const update = {
      $unset: {
        receivedTime: '',
        nextReceivableTime: '',
      },
      $set: {
        rejectedTime: new Date(),
        rejectionReason: queueItem.rejectionReason,
      },
      $push: {
        releaseHistory: {
          retryCount: queueItem.retryCount ? queueItem.retryCount : 0,
          receivedTime: queueItem.receivedTime,
          releasedTime: new Date(),
          releasedReason: queueItem.releasedReason,
        },
      },
    };

    return _updateOneById(queueItem._id, update);
  }

  async function _receive() {
    const query = {
      type: {$in: _.keys(_workers)},
      rejectedTime: {$exists: false},
      $and: [
        {
          $or: [{nextReceivableTime: {$lt: new Date()}}, {nextReceivableTime: {$exists: false}}],
        },
        {
          $or: [{receivedTime: {$lt: new Date(Date.now() - self.processingTimeout)}}, {receivedTime: {$exists: false}}],
        },
      ],
    };
    const update = {
      $set: {
        receivedTime: new Date(),
      },
    };

    const collection = await _getCollection();
    const result = await collection.findOneAndUpdate(query, update, {returnDocument: 'after', sort: 'priority'});

    if (result.value !== undefined) {
      return result.value;
    }

    return result;
  }

  function _buildQueueItemQuery(type, messageQuery) {
    const query = {type};

    _.forEach(messageQuery, function (value, key) {
      const property = `message.${key}`;
      query[property] = value;
    });

    return query;
  }

  function _buildQueueItemUpdate(messageUpdate, options) {
    const update = {};
    const $set = {};

    if (options.nextReceivableTime) {
      $set.nextReceivableTime = options.nextReceivableTime;
    }

    _.forEach(messageUpdate, function (value, key) {
      const property = `message.${key}`;
      $set[property] = value;
    });

    if (!_.isEmpty($set)) {
      update.$set = $set;
    }

    return update;
  }

  async function _getCollection() {
    if (!self.databasePromise) {
      throw new Error('No database configured');
    }
    const db = await self.databasePromise();
    return db.collection(self.collectionName);
  }

  async function _removeOne(query) {
    const collection = await _getCollection();
    const result = await collection.deleteOne(query);
    return result.deletedCount;
  }

  function _removeOneById(id) {
    return _removeOne({_id: id});
  }

  async function _removeMany(query) {
    const collection = await _getCollection();
    const result = await collection.deleteMany(query);
    return result.deletedCount;
  }

  async function _updateOne(query, update) {
    const collection = await _getCollection();
    const result = await collection.updateOne(query, update);
    return result.modifiedCount;
  }

  function _updateOneById(id, update) {
    return _updateOne({_id: id}, update);
  }

  async function _updateMany(query, update) {
    const collection = await _getCollection();
    const result = await collection.updateMany(query, update);
    return result.modifiedCount;
  }

  //endregion
}

module.exports = MessageQueue;
