var sinon = require('sinon');
require('sinon-as-promised');

function MessageQueueStubs(mQueue) {
  var self = this;

  self.attach = function() {
    self.errorHandlerStub = sinon.stub(mQueue, 'errorHandler');

    self._startPollingStub = sinon.stub(mQueue, '_startPolling');
    self._stopPollingStub = sinon.stub(mQueue, '_stopPolling');
    self._pollStub = sinon.stub(mQueue, '_poll');
    self._processStub = sinon.stub(mQueue, '_process');
    self._enqueueStub = sinon.stub(mQueue, '_enqueue');
    self._dequeueStub = sinon.stub(mQueue, '_dequeue');
    self._releaseStub = sinon.stub(mQueue, '_release');
    self._rejectStub = sinon.stub(mQueue, '_reject');
    self._receiveStub = sinon.stub(mQueue, '_receive');

    self._processStub.resolves();
    self._enqueueStub.resolves();
    self._dequeueStub.resolves();
    self._releaseStub.resolves();
    self._rejectStub.resolves();
    self._receiveStub.resolves();
  };

  self.reset = function() {
    self.errorHandlerStub.reset();

    self._startPollingStub.reset();
    self._stopPollingStub.reset();
    self._pollStub.reset();
    self._processStub.reset();
    self._enqueueStub.reset();
    self._dequeueStub.reset();
    self._releaseStub.reset();
    self._rejectStub.reset();
    self._receiveStub.reset();
  };

  self.restore = function() {
    mQueue.errorHandler.restore();

    mQueue._startPolling.restore();
    mQueue._stopPolling.restore();
    mQueue._poll.restore();
    mQueue._process.restore();
    mQueue._enqueue.restore();
    mQueue._dequeue.restore();
    mQueue._release.restore();
    mQueue._reject.restore();
    mQueue._receive.restore();
  };
}

module.exports = MessageQueueStubs;
