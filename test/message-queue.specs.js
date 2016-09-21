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
    return MongoClient.connect('mongodb://localhost/MessageQueue_TEST')
      .then(function(database) {
        db = database;
        _queueCollection = db.collection('_queue');
      });
  });
  beforeEach(function() {
    return _queueCollection.deleteMany({});
  });
  afterEach(function() {
    mQueue.databasePromise = null;
    mQueue.pollingInterval = 1000;
    mQueue.processingTimeout = 30 * 1000;
    mQueue.stopPolling();
    mQueue._workers = {};
    mQueue._numWorkers = 0;
  });

  var mockMessageType = 'doSomething';
  var mockMessage = {
    operation: 'something'
  };

  function _ensureDatabasePromise() {
    mQueue.databasePromise = function() {
      return Q.when(db);
    };
  }

  function _expectPromiseToThrowError(promise, errorMessage) {
    return promise
      .then(assert.fail)
      .catch(function(err) {
        expect(err).to.have.property('message', errorMessage);
      });
  }

  function _enqueueAndGetFromDatabase(type, message, options) {
    _ensureDatabasePromise();
    return mQueue.enqueue(type, message, options)
      .then(function(returnedDocument) {
        return [returnedDocument, _queueCollection.find({}).toArray()];
      })
      .spread(function(returnedDocument, allDocuments) {
        expect(allDocuments.length).to.equal(1);
        var savedDocument = allDocuments[0];
        expect(savedDocument._id).to.deep.equal(returnedDocument._id);
        return savedDocument;
      });
  }

  function _rejectAndGetFromDatabase(queueItem) {
    _ensureDatabasePromise();
    return mQueue._reject(queueItem)
      .then(function() {
        return _queueCollection.findOne({_id: queueItem._id});
      });
  }

  function _enqueueAndPollAndGetFromDatabase(type, message, options) {
    _ensureDatabasePromise();
    return mQueue.enqueue(type, message, options)
      .then(function(returnedDocument) {
        return [returnedDocument, mQueue._poll()];
      })
      .spread(function(returnedDocument) {
        return _queueCollection.findOne({_id: returnedDocument._id});
      });
  }

  describe('should accept a message for processing (.enqueue)', function() {
    specify('requiring a database connection', function() {
      mQueue.databasePromise = null;
      return _expectPromiseToThrowError(mQueue.enqueue(mockMessageType, mockMessage), 'No database configured');
    });
    specify('requiring a message type');
    describe('and allow the message to be processed as soon as possible (by default)', function() {
      specify('by saving a document to the database without a nextReceivableTime', function() {
        return _enqueueAndGetFromDatabase(mockMessageType, mockMessage)
          .then(function(savedDocument) {
            expect(savedDocument).to.have.property('type', mockMessageType);
            expect(savedDocument).to.have.property('message');
            expect(savedDocument.message).to.deep.equal(mockMessage);
            expect(savedDocument).to.not.have.property('nextReceivableTime');
          });
      });
    });
    describe('and allow the message to be processed at a specific future date/time', function() {
      specify('by saving a document to the database with the specified nextReceivableTime', function() {
        var options = {nextReceivableTime: moment().toDate()};
        return _enqueueAndGetFromDatabase(mockMessageType, mockMessage, options)
          .then(function(savedDocument) {
            expect(savedDocument).to.have.property('type', mockMessageType);
            expect(savedDocument).to.have.property('message');
            expect(savedDocument.message).to.deep.equal(mockMessage);
            expect(savedDocument).to.have.property('nextReceivableTime');
            expect(savedDocument.nextReceivableTime).to.equalTime(options.nextReceivableTime);
          });
      });
    });
    describe('and record the date/time it was accepted', function() {
      specify('by adding a dateCreated field to the document saved to the database', function() {
        var startTime = moment();
        return _enqueueAndGetFromDatabase(mockMessageType, mockMessage)
          .then(function(savedDocument) {
            var endTime = moment();
            expect(savedDocument).to.have.property('dateCreated');
            expect(savedDocument.dateCreated).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
            expect(savedDocument.dateCreated).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
          });
      });
    });
  });

  describe('should look for messages to process (._poll, ._receive)', function() {
    var clock;
    afterEach(function() {
      stubs.restore();
      if (clock && _.isFunction(clock.restore)) {
        clock.restore();
      }
    });
    specify('at a configurable interval after calling .registerWorker (._startPolling)', function() {
      clock = sinon.useFakeTimers();
      stubs.attach('_poll');
      mQueue.pollingInterval = 10 * 1000;
      mQueue.registerWorker(mockMessageType, sinon.stub());
      clock.tick(mQueue.pollingInterval * 5);
      expect(stubs.get('_poll').callCount).to.equal(5);
    });
    specify('immediately again after a message is successfully processed', function() {
      var originalPollMethod = mQueue._poll;
      clock = sinon.useFakeTimers();
      stubs.attach(['_poll', '_receive', '_process']);
      stubs.get('_receive').resolves({});
      stubs.get('_process').resolves();
      return originalPollMethod()
        .then(function() {
          expect(stubs.get('_poll').callCount).to.equal(0);
          clock.tick(0);
          expect(stubs.get('_poll').callCount).to.equal(1);
          clock.tick(mQueue.pollingInterval * 5);
          expect(stubs.get('_poll').callCount).to.equal(1);
        });
    });
    specify('as long as the queue is not at its configurable processing limit', function() {
      clock = sinon.useFakeTimers();
      stubs.attach(['_receive', '_dequeue', '_startPolling']);
      var worker = function() {
        return Q.Promise(function(resolve) {
          setTimeout(function() {
            resolve('Completed');
          }, 100);
        });
      };
      stubs.get('_receive').resolves();
      var processingPromises = [];
      mQueue.maxWorkers = 2;
      mQueue.registerWorker(mockMessageType, worker);
      processingPromises.push(mQueue._process({type: mockMessageType}));
      expect(mQueue._numWorkers).to.equal(1);
      return Q.when(0)
        .then(function() {
          return mQueue._poll();
        })
        .then(function() {
          expect(stubs.get('_receive').callCount).to.equal(1);
          stubs.get('_receive').reset();
          processingPromises.push(mQueue._process({type: mockMessageType}));
          expect(mQueue._numWorkers).to.equal(2);
          return mQueue._poll();
        })
        .then(function() {
          expect(stubs.get('_receive').callCount).to.equal(0);
          clock.tick(100);
          return processingPromises;
        })
        .spread(function() {
          expect(mQueue._numWorkers).to.equal(0);
          return mQueue._poll();
        }).then(function() {
          expect(stubs.get('_receive').callCount).to.equal(1);
        });
    });
    describe('ignoring messages', function() {
      specify('whose type does’t have a registered processing method', function() {
        stubs.attach(['_process', '_startPolling']);
        stubs.get('_process').resolves();
        mQueue.registerWorker(mockMessageType, sinon.stub());
        return _enqueueAndPollAndGetFromDatabase('other', mockMessage)
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(false);
            return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage);
          })
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(true);
          });
      });
      specify('that shouldn’t be processed until a future date/time', function() {
        stubs.attach(['_process', '_startPolling']);
        stubs.get('_process').resolves();
        mQueue.registerWorker(mockMessageType, sinon.stub());
        var options = {nextReceivableTime: moment().add(30, 'seconds').toDate()};
        return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage, options)
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(false);
            options = {nextReceivableTime: moment().add(-1, 'seconds').toDate()};
            return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage, options);
          })
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(true);
          });
      });
      specify('that have previously been marked as rejected', function() {
        stubs.attach(['_process', '_startPolling']);
        stubs.get('_process').resolves();
        mQueue.registerWorker(mockMessageType, sinon.stub());
        return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage)
          .then(function(queueItem) {
            expect(queueItem).to.not.have.property('rejectedTime');
            expect(stubs.get('_process').calledOnce).to.equal(true);
            stubs.get('_process').reset();
            return _rejectAndGetFromDatabase(queueItem);
          })
          .then(function(queueItem) {
            expect(queueItem).to.have.property('rejectedTime');
            return mQueue._poll();
          })
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(false);
          });
      });
      specify('that are currently being processed', function() {
        stubs.attach(['_process', '_startPolling']);
        stubs.get('_process').resolves();
        mQueue.registerWorker(mockMessageType, sinon.stub());
        return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage)
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(true);
            stubs.get('_process').reset();
            return mQueue._poll();
          })
          .then(function() {
            expect(stubs.get('_process').calledOnce).to.equal(false);
          });
      });
      describe('unless the message currently being processed become stuck', function() {
        specify('by being older than a configurable processing timeout', function() {
          clock = sinon.useFakeTimers(moment().toDate().getTime());
          stubs.attach(['_process', '_startPolling']);
          stubs.get('_process').resolves();
          mQueue.processingTimeout = 60 * 1000;
          mQueue.registerWorker(mockMessageType, sinon.stub());
          return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage)
            .then(function() {
              expect(stubs.get('_process').calledOnce).to.equal(true);
              stubs.get('_process').reset();
              clock.tick(mQueue.processingTimeout + 1);
              return mQueue._poll();
            })
            .then(function() {
              expect(stubs.get('_process').calledOnce).to.equal(true);
            });
        });
      });
    });
    describe('and record the date/time it was received for processing', function() {
      specify('by adding a receivedTime field to the document in the database', function() {
        stubs.attach(['_process', '_startPolling']);
        stubs.get('_process').resolves();
        mQueue.registerWorker(mockMessageType, sinon.stub());
        var startTime = moment();
        return _enqueueAndPollAndGetFromDatabase(mockMessageType, mockMessage)
          .then(function(savedDocument) {
            var endTime = moment();
            expect(stubs.get('_process').calledOnce).to.equal(true);
            expect(savedDocument).to.have.property('receivedTime');
            expect(savedDocument.receivedTime).to.be.afterTime(startTime.add(-1, 'seconds').toDate());
            expect(savedDocument.receivedTime).to.be.beforeTime(endTime.add(1, 'seconds').toDate());
          });
      });
    });
    describe('and report any thrown errors', function() {
      specify('by passing them to .errorHandler', function() {
        stubs.attach(['errorHandler', '_receive']);
        var error = new Error('NOPE!');
        stubs.get('_receive').rejects(error);
        return mQueue._poll()
          .then(function() {
            expect(stubs.get('errorHandler').callCount).to.equal(1);
            expect(stubs.get('errorHandler').firstCall.args[0]).to.equal(error);
          });
      });
    })
  });

  describe('should successfully process a message', function() {
    specify('with the registered processing method for the message’s type');
    specify('after a previous processing failure');
    specify('and remove it after it has been processed');
  });

  describe('should handle processing failures', function() {
    specify('recording the date/time and failure reason');
    specify('and allow the message to be processed again at a specific future date/time');
    specify('and be able to mark the message as rejected');
  });
})
;