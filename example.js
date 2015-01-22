'use strict';

var Agent = require('./'),
    BPromise = require('bluebird'),
    agent;


agent = new Agent({
  identify_host: 'localhost:9000',
  engines: [{
    name: 'rabbit',
    engine: require('../governor-work-rabbitmq'),
    options: {
      connectionUrl: 'amqp://localhost',
      pool: {
        min: 0,
        max: 60
      }
    }
  }],
  log: require('../governor-log-bunyan').generate()
});

agent.consume({
  job: 'somesweetname',
  source: {
    engine: 'rabbit',
    options: {
      fixture: function (channel, consumeCallback) {
        //setup channel
        var exchangeName = 'ha.hms.events';

        return BPromise.props({
          exchange: channel.assertExchange(exchangeName, 'topic', {durable: true}),
          prefetch: channel.prefetch(1),
          queue: channel.assertQueue('ha.events.property', {autoDelete: false, durable: true})
        }).tap(function (result) {
          return channel.bindQueue(result.queue.queue, exchangeName, 'property.detail');
        }).then(function (result) {
          return channel.consume(result.queue.queue, consumeCallback, {noAck: false});
        });
      }
    }
  },

  //inquire: {
  //    locking: true,
  //    key: function (task) {
  //        return 'somesweetnameagain-' + task.zip_code;
  //    }
  //},

  //inquire: function (task, governor) {
  //  // returns promise with an object representing result
  //  // governor.continue() === governor.ok()
  //  // governor.locked([reason])
  //  // governor.invalid([reason])
  //
  //  return Promise.resolve().then(function () {
  //
  //    return governor.isLocked(task.name);
  //  }).then(function (locked) {
  //
  //    if (locked) {
  //      return governor.locked();
  //    }
  //
  //    return governor.lock(task.name).then(function () {
  //      return governor.ok();
  //    });
  //  })
  //},

  worker: function (task, governor) {

  }

});
