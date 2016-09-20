var _ = require('lodash');
var Q = require('q');
var chai = require('chai');
var expect = chai.expect;
var assert = chai.assert;
chai.use(require('chai-datetime'));
var moment = require('moment');
var sinon = require('sinon');

var MongoClient = require('mongodb').MongoClient;

var MessageQueue = require('../lib/message-queue');
var MessageQueueStubs = require('./message-queue.stubs');

describe('MessageQueue', function() {
  var mQueue = new MessageQueue();
  var stubs = new MessageQueueStubs(mQueue);

  var db, _queueCollection;
  before(function() {
    stubs.attach();
    return MongoClient.connect('mongodb://localhost/MessageQueue_TEST')
      .then(function(database) {
        db = database;
        _queueCollection = db.collection('_queue');
      });
  });
  beforeEach(function() {
    stubs.reset();
    return _queueCollection.deleteMany({});
  });
  afterEach(function() {
    mQueue._workers = {};
    mQueue._numWorkers = 0;
    mQueue._pollingIntervalId = null;
  });
  after(function() {
    stubs.restore();
  });

  function _ensureDatabasePromise() {
    mQueue.databasePromise = function() {
      return Q.when(db);
    };
  }

  it('should set appropriate default values', function() {
    expect(mQueue).to.have.property('collectionName', '_queue');
    expect(mQueue).to.have.property('pollingInterval', 1000);
    expect(mQueue).to.have.property('processingTimeout', 30 * 1000);
    expect(mQueue).to.have.property('maxWorkers', 5);
  });

  context('Public Methods', function() {

    describe('.registerWorker', function() {
      it('should add the worker to the private _workers object', function() {
        expect(_.isUndefined(mQueue._workers.doSomething)).to.equal(true);
        var worker = sinon.stub();
        mQueue.registerWorker('doSomething', worker);
        expect(_.isUndefined(mQueue._workers.doSomething)).to.equal(false);
        expect(mQueue._workers.doSomething).to.equal(worker);
        expect(worker.calledOnce).to.equal(false);
      });
      it('should call the private _startPolling method', function() {
        var worker = sinon.stub();
        mQueue.registerWorker('doSomething', worker);
        expect(stubs._startPollingStub.calledOnce).to.equal(true);
      });
    });

    describe('.stopPolling', function() {
      it('should call the private _stopPolling method', function() {
        mQueue.stopPolling();
        expect(stubs._stopPollingStub.calledOnce).to.equal(true);
      });
    });

    describe('.enqueue', function() {
      describe('should call the private _enqueue helper method', function() {
        specify('with the requested type and message fields', function() {
          var message = {operation: 'something'};
          return mQueue.enqueue('doSomething', message)
            .then(function() {
              expect(stubs._enqueueStub.calledOnce).to.equal(true);
              var args = stubs._enqueueStub.firstCall.args;
              expect(args[0]).to.have.property('type', 'doSomething');
              expect(args[0]).to.have.property('message');
              expect(args[0].message).to.deep.equal(message);
            });
        });
        specify('with a dateCreated field', function() {
          var message = {operation: 'something'};
          var startTime = moment();
          return mQueue.enqueue('doSomething', message)
            .then(function() {
              var endTime = moment();
              expect(stubs._enqueueStub.calledOnce).to.equal(true);
              var args = stubs._enqueueStub.firstCall.args;
              expect(args[0]).to.have.property('dateCreated');
              expect(args[0].dateCreated).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(args[0].dateCreated).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
            });
        });
        specify('with a nextReceivableTime field (if one was supplied in the optional options argument)', function() {
          var message = {operation: 'something'};
          var options = {nextReceivableTime: moment().add(30, 'seconds').toDate()};
          return mQueue.enqueue('doSomething', message, options)
            .then(function() {
              expect(stubs._enqueueStub.calledOnce).to.equal(true);
              var args = stubs._enqueueStub.firstCall.args;
              expect(args[0]).to.have.property('nextReceivableTime');
              expect(args[0].nextReceivableTime).to.equalTime(options.nextReceivableTime);
            });
        });
      });
    });

    describe('.enqueueAndProcess', function() {
      describe('should call the private _enqueue helper method', function() {
        specify('with the requested type and message fields', function() {
          var message = {operation: 'something'};
          return mQueue.enqueueAndProcess('doSomething', message)
            .then(function() {
              expect(stubs._enqueueStub.calledOnce).to.equal(true);
              var args = stubs._enqueueStub.firstCall.args;
              expect(args[0]).to.have.property('type', 'doSomething');
              expect(args[0]).to.have.property('message');
              expect(args[0].message).to.deep.equal(message);
            });
        });
        specify('with a dateCreated field', function() {
          var message = {operation: 'something'};
          var startTime = moment();
          return mQueue.enqueueAndProcess('doSomething', message)
            .then(function() {
              var endTime = moment();
              expect(stubs._enqueueStub.calledOnce).to.equal(true);
              var args = stubs._enqueueStub.firstCall.args;
              expect(args[0]).to.have.property('dateCreated');
              expect(args[0].dateCreated).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(args[0].dateCreated).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
            });
        });
        specify('with a receivedTime field', function() {
          var message = {operation: 'something'};
          var startTime = moment();
          return mQueue.enqueueAndProcess('doSomething', message)
            .then(function() {
              var endTime = moment();
              expect(stubs._enqueueStub.calledOnce).to.equal(true);
              var args = stubs._enqueueStub.firstCall.args;
              expect(args[0]).to.have.property('receivedTime');
              expect(args[0].receivedTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(args[0].receivedTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
            });
        });
      });
      describe('should call the private _process helper method', function() {
        specify('with the document that _enqueue returns', function() {
          var message = {operation: 'something'};
          var returnedDocument = {
            _id: 'returned.document',
            type: 'doSomething',
            dateCreated: moment().toDate(),
            message: {
              operation: 'something'
            },
            receivedTime: moment().toDate()
          };
          stubs._enqueueStub.resolves(returnedDocument);
          return mQueue.enqueueAndProcess('doSomething', message)
            .then(function() {
              expect(stubs._processStub.calledOnce).to.equal(true);
              var args = stubs._processStub.firstCall.args;
              expect(args[0]).to.deep.equal(returnedDocument);
            });
        });
      });
    });
  });

  context('Private Helper Methods', function() {

    var mockQueueItem = {
      type: 'doSomething',
      dateCreated: moment().toDate(),
      message: {
        operation: 'something'
      }
    };

    function _insertQueueItem(queueItem) {
      delete queueItem._id;
      return _queueCollection.insertOne(queueItem)
        .then(function(result) {
          return result.ops[0];
        });
    }

    describe('._startPolling', function() {

      var setIntervalStub;
      before(function() {
        setIntervalStub = sinon.stub(global, 'setInterval');
        mQueue._startPolling.restore();
      });
      beforeEach(function() {
        setIntervalStub.reset();
      });
      after(function() {
        stubs._startPollingStub = sinon.stub(mQueue, '_startPolling');
        global.setInterval.restore();
      });

      it('should start a new interval timer (if one was not already started)', function() {
        mQueue._pollingIntervalId = null;
        setIntervalStub.returns('new polling interval');
        mQueue._startPolling();
        expect(setIntervalStub.calledOnce).to.equal(true);
        var args = setIntervalStub.firstCall.args;
        expect(args[0]).to.equal(mQueue._poll);
        expect(args[1]).to.equal(mQueue.pollingInterval);
        expect(mQueue).to.have.property('_pollingIntervalId', 'new polling interval');
      });
      it('should not start a new interval timer (if one was already started)', function() {
        mQueue._pollingIntervalId = 'old polling interval';
        mQueue._startPolling();
        expect(setIntervalStub.calledOnce).to.equal(false);
        expect(mQueue).to.have.property('_pollingIntervalId', 'old polling interval');
      });
    });

    describe('._stopPolling', function() {

      var clearIntervalStub;
      before(function() {
        mQueue._stopPolling.restore();
        clearIntervalStub = sinon.stub(global, 'clearInterval');
      });
      beforeEach(function() {
        clearIntervalStub.reset();
      });
      after(function() {
        stubs._stopPollingStub = sinon.stub(mQueue, '_stopPolling');
        global.clearInterval.restore();
      });

      it('should stop the interval timer (if one was already started)', function() {
        mQueue._pollingIntervalId = 'polling interval';
        mQueue._stopPolling();
        expect(clearIntervalStub.calledOnce).to.equal(true);
        expect(clearIntervalStub.firstCall.args[0]).to.equal(mQueue._pollingIntervalId);
      });
    });

    describe('._startPolling and _.stopPolling', function() {

      var clock;
      before(function() {
        mQueue._startPolling.restore();
        mQueue._stopPolling.restore();
        clock = sinon.useFakeTimers();
      });
      after(function() {
        mQueue._stopPolling();
        clock.restore();
        stubs._startPollingStub = sinon.stub(mQueue, '_startPolling');
        stubs._stopPollingStub = sinon.stub(mQueue, '_stopPolling');
      });

      it('should trigger the private _poll method every pollingInterval (until stopped)', function() {
        mQueue._startPolling();
        clock.tick(mQueue.pollingInterval * 5);
        expect(stubs._pollStub.callCount).to.equal(5);
        stubs._pollStub.reset();

        mQueue._stopPolling();
        clock.tick(mQueue.pollingInterval * 5);
        expect(stubs._pollStub.callCount).to.equal(0);
      });
    });

    describe('._poll', function() {

      var setImmediateStub;
      beforeEach(function() {
        mQueue._poll.restore();
        setImmediateStub = sinon.stub(global, 'setImmediate');
      });
      afterEach(function() {
        stubs._pollStub = sinon.stub(mQueue, '_poll');
        stubs._pollStub.resolves();
        global.setImmediate.restore();
      });

      it('should check for more work by calling the private _receive method', function() {
        return mQueue._poll()
          .then(function() {
            expect(stubs._receiveStub.calledOnce).to.equal(true);
          });
      });
      it('should not check for more work if _numWorkers >= maxWorkers', function() {
        mQueue._numWorkers = mQueue.maxWorkers;
        mQueue._poll();
        expect(stubs._receiveStub.calledOnce).to.equal(false);
      });
      it('should process a received work item by passing it to the private _process method', function() {
        var workItem = _.cloneDeep(mockQueueItem);
        stubs._receiveStub.resolves(workItem);
        return mQueue._poll()
          .then(function() {
            expect(stubs._processStub.calledOnce).to.equal(true);
            var args = stubs._processStub.firstCall.args;
            expect(args[0]).to.deep.equal(workItem);
          });
      });
      it('should look for more work immediately (if it just processed something', function() {
        var workItem = _.cloneDeep(mockQueueItem);
        stubs._receiveStub.resolves(workItem);
        return mQueue._poll()
          .then(function() {
            expect(setImmediateStub.calledOnce).to.equal(true);
            expect(setImmediateStub.firstCall.args[0]).to.equal(mQueue._poll);
          });
      });
      it('should trap any errors and call the errorHandler instead', function() {
        var error = new Error('FAILURE!');
        stubs._receiveStub.rejects(error);
        return mQueue._poll()
          .then(function() {
            expect(stubs.errorHandlerStub.calledOnce).to.equal(true);
            expect(stubs.errorHandlerStub.firstCall.args[0]).to.equal(error);
          });
      });
    });

    describe('._process', function() {

      beforeEach(function() {
        mQueue._process.restore();
      });
      afterEach(function() {
        stubs._processStub = sinon.stub(mQueue, '_process');
        stubs._processStub.resolves();
      });

      it('should throw an error if a worker for the type has not been registered', function() {
        return mQueue._process({type: 'doSomething'})
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'No worker registered for type: doSomething');
          });
      });
      it('should increment _numWorkers prior to calling the worker', function() {
        var intermediateNumWorkers;
        mQueue._workers.doSomething = function() {
          return Q.Promise(function(resolve, reject) {
            intermediateNumWorkers = mQueue._numWorkers;
            resolve('Completed');
          });
        };
        expect(mQueue._numWorkers).to.equal(0);
        return mQueue._process({type: 'doSomething'})
          .then(function() {
            expect(intermediateNumWorkers).to.equal(1);
          })
      });
      it('should decrement _numWorkers after calling the worker', function() {
        var intermediateNumWorkers;
        mQueue._workers.doSomething = function() {
          return Q.Promise(function(resolve, reject) {
            intermediateNumWorkers = mQueue._numWorkers;
            resolve('Completed');
          });
        };
        expect(mQueue._numWorkers).to.equal(0);
        return mQueue._process({type: 'doSomething'})
          .then(function() {
            expect(intermediateNumWorkers).to.equal(1);
            expect(mQueue._numWorkers).to.equal(0);
          });
      });
      it('should call the worker', function() {
        var queueItem = {type: 'doSomething'};
        var workerStub = mQueue._workers.doSomething = sinon.stub();
        workerStub.resolves('Completed');
        return mQueue._process(queueItem)
          .then(function() {
            expect(workerStub.calledOnce).to.equal(true);
            expect(workerStub.firstCall.args[0]).to.equal(queueItem);
          })
      });
      it('should send the queueItem to the private _dequeue method if the worker returns a Completed status', function() {
        var queueItem = {type: 'doSomething'};
        var workerStub = mQueue._workers.doSomething = sinon.stub();
        workerStub.resolves('Completed');
        return mQueue._process(queueItem)
          .then(function() {
            expect(stubs._dequeueStub.calledOnce).to.equal(true);
            expect(stubs._dequeueStub.firstCall.args[0]).to.equal(queueItem);
          })
      });
      it('should send the queueItem to the private _release method if the worker returns a Retry status', function() {
        var queueItem = {type: 'doSomething'};
        var workerStub = mQueue._workers.doSomething = sinon.stub();
        workerStub.resolves('Retry');
        return mQueue._process(queueItem)
          .then(function() {
            expect(stubs._releaseStub.calledOnce).to.equal(true);
            expect(stubs._releaseStub.firstCall.args[0]).to.equal(queueItem);
          })
      });
      it('should send the queueItem to the private _rejected method if the worker returns a Rejected status', function() {
        var queueItem = {type: 'doSomething'};
        var workerStub = mQueue._workers.doSomething = sinon.stub();
        workerStub.resolves('Rejected');
        return mQueue._process(queueItem)
          .then(function() {
            expect(stubs._rejectStub.calledOnce).to.equal(true);
            expect(stubs._rejectStub.firstCall.args[0]).to.equal(queueItem);
          })
      });
      it('should throw an error if the worker returns an unknown status', function() {
        var queueItem = {type: 'doSomething'};
        var workerStub = mQueue._workers.doSomething = sinon.stub();
        workerStub.resolves();
        return mQueue._process(queueItem)
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'Unknown status: undefined');
          });
      });
    });

    describe('._enqueue', function() {

      beforeEach(function() {
        mQueue._enqueue.restore();
      });
      afterEach(function() {
        stubs._enqueueStub = sinon.stub(mQueue, '_enqueue');
        stubs._enqueueStub.resolves();
      });

      function __enqueueHelper(queueItem) {
        _ensureDatabasePromise();
        var returnedRecord;
        return mQueue._enqueue(queueItem)
          .then(function(record) {
            returnedRecord = record;
            return _queueCollection.find({}).toArray();
          })
          .then(function(results) {
            expect(results.length).to.equal(1);
            var savedRecord = results[0];
            return [savedRecord, returnedRecord];
          });
      }

      it('should throw an error if databasePromise is not yet configured', function() {
        mQueue.databasePromise = null;
        return mQueue._enqueue({})
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'No database configured');
          });
      });
      it('should save a document to the database', function() {
        return __enqueueHelper(mockQueueItem)
          .spread(function(savedRecord) {
            expect(savedRecord).to.have.property('type', mockQueueItem.type);
            expect(savedRecord).to.have.property('dateCreated');
            expect(savedRecord.dateCreated).to.equalTime(mockQueueItem.dateCreated);
            expect(savedRecord).to.have.property('message');
            expect(savedRecord.message).to.deep.equal(mockQueueItem.message);
          });
      });
      it('should return the saved document', function() {
        return __enqueueHelper(mockQueueItem)
          .spread(function(savedRecord, returnedRecord) {
            expect(returnedRecord).to.have.property('_id');
            expect(returnedRecord._id.toString()).to.equal(savedRecord._id.toString());
            expect(returnedRecord).to.have.property('type', savedRecord.type);
            expect(returnedRecord).to.have.property('dateCreated');
            expect(returnedRecord.dateCreated).to.equalTime(savedRecord.dateCreated);
            expect(returnedRecord).to.have.property('message');
            expect(returnedRecord.message).to.deep.equal(savedRecord.message);
          });
      });
    });

    describe('._dequeue', function() {

      beforeEach(function() {
        mQueue._dequeue.restore();
      });
      afterEach(function() {
        stubs._dequeueStub = sinon.stub(mQueue, '_dequeue');
        stubs._dequeueStub.resolves();
      });

      it('should throw an error if databasePromise is not yet configured', function() {
        mQueue.databasePromise = null;
        return mQueue._dequeue({})
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'No database configured');
          });
      });
      it('should remove the document from the database', function() {
        _ensureDatabasePromise();
        var insertedRecord;
        return _insertQueueItem(mockQueueItem)
          .then(function(record) {
            insertedRecord = record;
            return _queueCollection.find({}).toArray();
          })
          .then(function(results) {
            expect(results.length).to.equal(1);
            expect(results[0]._id.toString()).to.equal(insertedRecord._id.toString());
            return mQueue._dequeue(insertedRecord);
          })
          .then(function() {
            return _queueCollection.find({}).toArray();
          })
          .then(function(results) {
            expect(results.length).to.equal(0);
          });
      });
    });

    describe('._release', function() {

      beforeEach(function() {
        mQueue._release.restore();
      });
      afterEach(function() {
        stubs._releaseStub = sinon.stub(mQueue, '_release');
        stubs._releaseStub.resolves();
      });

      function __releaseHelper(queueItem, modifications) {
        _ensureDatabasePromise();
        var insertedRecord;
        return _insertQueueItem(queueItem)
          .then(function(record) {
            insertedRecord = record;
            return mQueue._release(_.merge(_.cloneDeep(queueItem), modifications || {}));
          })
          .then(function() {
            return _queueCollection.findOne({_id: insertedRecord._id});
          });
      }

      it('should throw an error if databasePromise is not yet configured', function() {
        mQueue.databasePromise = null;
        return mQueue._release({})
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'No database configured');
          });
      });
      describe('should update the document in the database', function() {
        specify('by removing the receivedTime field', function() {
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.receivedTime = moment().toDate();
          return __releaseHelper(queueItem)
            .then(function(updatedRecord) {
              expect(updatedRecord).to.not.have.property('receivedTime');
            });
        });
        specify('by adding a retryCount field (if it doesn\'t already exist)', function() {
          return __releaseHelper(mockQueueItem)
            .then(function(updatedRecord) {
              expect(updatedRecord).to.have.property('retryCount', 1);
            });
        });
        specify('by incrementing the retryCount field (if it already exists)', function() {
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.retryCount = 2;
          return __releaseHelper(queueItem)
            .then(function(updatedRecord) {
              expect(updatedRecord).to.have.property('retryCount', 3);
            });
        });
        specify('by adding a nextReceivableTime field (if it wasn\'t passed in)', function() {
          var startTime = moment();
          return __releaseHelper(mockQueueItem)
            .then(function(updatedRecord) {
              var endTime = moment();
              expect(updatedRecord).to.have.property('nextReceivableTime');
              expect(updatedRecord.nextReceivableTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(updatedRecord.nextReceivableTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
            });
        });
        specify('by setting the nextReceivableTime field (if it was passed in)', function() {
          var modifications = {nextReceivableTime: moment().add(30, 'seconds').toDate()};
          return __releaseHelper(mockQueueItem, modifications)
            .then(function(updatedRecord) {
              expect(updatedRecord).to.have.property('nextReceivableTime');
              expect(updatedRecord.nextReceivableTime).to.equalTime(modifications.nextReceivableTime);
            });
        });
        specify('by pushing a new record to the releaseHistory array', function() {
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.receivedTime = moment().toDate();
          queueItem.retryCount = 1;
          queueItem.releaseHistory = [{
            retryCount: 0,
            receivedTime: moment().add(-30, 'seconds').toDate(),
            releasedTime: moment().add(-20, 'seconds').toDate(),
            releasedReason: 'NOPE!'
          }];
          var modifications = {releasedReason: 'Not this time!'};
          var startTime = moment();
          return __releaseHelper(queueItem, modifications)
            .then(function(updatedRecord) {
              var endTime = moment();
              expect(updatedRecord).to.have.property('releaseHistory');
              expect(updatedRecord.releaseHistory).to.have.length(2);
              expect(updatedRecord.releaseHistory[1]).to.have.property('retryCount', 1);
              expect(updatedRecord.releaseHistory[1]).to.have.property('receivedTime');
              expect(updatedRecord.releaseHistory[1].receivedTime).to.equalTime(queueItem.receivedTime);
              expect(updatedRecord.releaseHistory[1]).to.have.property('releasedTime');
              expect(updatedRecord.releaseHistory[1].releasedTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(updatedRecord.releaseHistory[1].releasedTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
              expect(updatedRecord.releaseHistory[1]).to.have.property('releasedReason', modifications.releasedReason);
            });
        });
      });
    });

    describe('._reject', function() {

      beforeEach(function() {
        mQueue._reject.restore();
      });
      afterEach(function() {
        stubs._rejectStub = sinon.stub(mQueue, '_reject');
        stubs._rejectStub.resolves();
      });

      function __rejectHelper(queueItem, modifications) {
        _ensureDatabasePromise();
        var insertedRecord;
        return _insertQueueItem(queueItem)
          .then(function(record) {
            insertedRecord = record;
            return mQueue._reject(_.merge(_.cloneDeep(queueItem), modifications || {}));
          })
          .then(function() {
            return _queueCollection.findOne({_id: insertedRecord._id});
          });
      }

      it('should throw an error if databasePromise is not yet configured', function() {
        mQueue.databasePromise = null;
        return mQueue._reject({})
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'No database configured');
          });
      });
      describe('should update the document in the database', function() {
        specify('by removing the receivedTime and nextReceivableTime fields', function() {
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.receivedTime = moment().toDate();
          return __rejectHelper(queueItem)
            .then(function(updatedRecord) {
              expect(updatedRecord).to.not.have.property('receivedTime');
              expect(updatedRecord).to.not.have.property('nextReceivableTime');
            });
        });
        specify('by setting the rejectedTime and rejectionReason fields', function() {
          var modifications = {rejectionReason: 'NOPE!'};
          var startTime = moment();
          return __rejectHelper(mockQueueItem, modifications)
            .then(function(updatedRecord) {
              var endTime = moment();
              expect(updatedRecord).to.have.property('rejectedTime');
              expect(updatedRecord.rejectedTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(updatedRecord.rejectedTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
              expect(updatedRecord).to.have.property('rejectionReason', modifications.rejectionReason);
            });
        });
        specify('by pushing a new record to the releaseHistory array', function() {
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.receivedTime = moment().toDate();
          queueItem.retryCount = 1;
          queueItem.releaseHistory = [{
            retryCount: 0,
            receivedTime: moment().add(-30, 'seconds').toDate(),
            releasedTime: moment().add(-20, 'seconds').toDate(),
            releasedReason: 'NOPE!'
          }];
          var modifications = {releasedReason: 'Not this time!'};
          var startTime = moment();
          return __rejectHelper(queueItem, modifications)
            .then(function(updatedRecord) {
              var endTime = moment();
              expect(updatedRecord).to.have.property('releaseHistory');
              expect(updatedRecord.releaseHistory).to.have.length(2);
              expect(updatedRecord.releaseHistory[1]).to.have.property('retryCount', 1);
              expect(updatedRecord.releaseHistory[1]).to.have.property('receivedTime');
              expect(updatedRecord.releaseHistory[1].receivedTime).to.equalTime(queueItem.receivedTime);
              expect(updatedRecord.releaseHistory[1]).to.have.property('releasedTime');
              expect(updatedRecord.releaseHistory[1].releasedTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(updatedRecord.releaseHistory[1].releasedTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
              expect(updatedRecord.releaseHistory[1]).to.have.property('releasedReason', modifications.releasedReason);
            });
        });
      });
    });

    describe('._receive', function() {

      beforeEach(function() {
        mQueue._receive.restore();
      });
      afterEach(function() {
        stubs._receiveStub = sinon.stub(mQueue, '_receive');
        stubs._receiveStub.resolves();
      });

      it('should throw an error if databasePromise is not yet configured', function() {
        mQueue.databasePromise = null;
        return mQueue._receive({})
          .then(assert.fail)
          .catch(function(err) {
            expect(err).to.have.property('message', 'No database configured');
          });
      });
      describe('should update the receivedTime and return the first matching document from the database', function() {

        function __receiveAndUpdateTester(queueItems, expectedUpdateIndex) {
          _ensureDatabasePromise();
          var startTime = moment();
          var _queueItems = [].concat(queueItems);
          return Q.all(_.map(_queueItems, _insertQueueItem))
            .then(function() {
              return mQueue._receive();
            })
            .then(function(result) {
              return [result, _queueCollection.find({}).toArray()];
            })
            .spread(function(result, documents) {
              var endTime = moment();
              expect(documents).to.have.length(_queueItems.length);
              var expectedUpdateDocument = documents[expectedUpdateIndex];
              expect(expectedUpdateDocument).to.have.property('receivedTime');
              expect(expectedUpdateDocument.receivedTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
              expect(expectedUpdateDocument.receivedTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
              expect(result).to.deep.equal(expectedUpdateDocument);
            });
        }

        specify('when there is a registered worker for the type', function() {
          mQueue._workers.doSomething = sinon.stub();
          return __receiveAndUpdateTester(mockQueueItem, 0);
        });
        specify('when the nextReceivableTime is in the past', function() {
          mQueue._workers.doSomething = sinon.stub();
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.nextReceivableTime = moment().add(-30, 'seconds').toDate();
          return __receiveAndUpdateTester(queueItem, 0);
        });
        specify('when the receivedTime is past the processingTimeout period', function() {
          mQueue._workers.doSomething = sinon.stub();
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.receivedTime = moment().add(-31, 'seconds').toDate();
          return __receiveAndUpdateTester(queueItem, 0);
        });
        specify('when there are multiple available documents', function() {
          mQueue._workers.doSomething = sinon.stub();
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.nextReceivableTime = moment().add(30, 'seconds').toDate();
          return __receiveAndUpdateTester([queueItem, mockQueueItem, mockQueueItem], 1);
        });
      });
      describe('should not update the receivedTime or return a document from the database', function() {

        function __receiveAndIgnoreTester(queueItem) {
          function _isUndefinedOrEqual(val1, val2) {
            return _.isUndefined(val1) || _.isEqual(val1, val2);
          }

          _ensureDatabasePromise();
          return Q.all([_insertQueueItem(queueItem)])
            .then(function() {
              return mQueue._receive();
            })
            .then(function(result) {
              return [result, _queueCollection.find({}).toArray()];
            })
            .spread(function(result, documents) {
              expect(documents).to.have.length(1);
              expect(_isUndefinedOrEqual(documents[0].receivedTime, queueItem.receivedTime)).to.equal(true);
              expect(_.isNull(result) || _.isUndefined(result)).to.equal(true);
            });
        }

        specify('when there is not a registered worker for the type', function() {
          mQueue._workers.doSomethingElse = sinon.stub();
          return __receiveAndIgnoreTester(mockQueueItem);
        });
        specify('when there is a rejectedTime on the document', function() {
          mQueue._workers.doSomething = sinon.stub();
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.rejectedTime = moment().add(-30, 'seconds').toDate();
          return __receiveAndIgnoreTester(queueItem);
        });
        specify('when the nextReceivableTime is in the future', function() {
          mQueue._workers.doSomething = sinon.stub();
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.nextReceivableTime = moment().add(30, 'seconds').toDate();
          return __receiveAndIgnoreTester(queueItem);
        });
        specify('when the receivedTime is within than the processingTimeout period', function() {
          mQueue._workers.doSomething = sinon.stub();
          var queueItem = _.cloneDeep(mockQueueItem);
          queueItem.receivedTime = moment().add(-29, 'seconds').toDate();
          return __receiveAndIgnoreTester(queueItem);
        });
      });
    });
  });
});
