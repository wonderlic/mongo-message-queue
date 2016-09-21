var _ = require('lodash');
var Q = require('q');
var sinon = require('sinon');
require('sinon-as-promised')(Q.Promise);

function MessageQueueStubs(mQueue) {
  var self = this;

  self.allMethods = [
    'errorHandler',
    '_startPolling',
    '_stopPolling',
    '_poll',
    '_process',
    '_enqueue',
    '_dequeue',
    '_release',
    '_reject',
    '_receive'
  ];

  self.get = function(method) {
    return self[method + 'Stub'];
  };

  self.attach = function(methods) {
    _.forEach([].concat(methods || self.allMethods), function(method) {
      self[method + 'Stub'] = sinon.stub(mQueue, method);
    });
  };

  self.reset = function(methods) {
    _.forEach([].concat(methods || self.allMethods), function(method) {
      self[method + 'Stub'].reset();
    });
  };

  self.restore = function(methods) {
    _.forEach([].concat(methods || self.allMethods), function(method) {
      if (_.isFunction(mQueue[method].restore)) {
        mQueue[method].restore();
      }
    });
  };
}

module.exports = MessageQueueStubs;
