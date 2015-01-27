'use strict';

var Joi = require('joi'),
    Hoek = require('hoek-boom'),
    Job = require('./job'),
    Net = require('./net'),
    util = require('util'),
    Lodash = require('lodash'),
    BPromise = require('bluebird'),
    Emitter = require('events').EventEmitter,
    internals = {};

/*
 agent = new GovernorAgent({
 host: 'localhost:9000',
 engines: [{
 name: 'rabbit',
 engine: require('governor-work-rabbitmq'),
 options: {
 pool: {
 min: 0,
 max: 60
 }
 }
 }],
 log: require('governor-log-bunyan')
 });
 */

function GovernorAgent (options) {
    var optionValidation,
        self = this;

    optionValidation = Joi.validate(options, internals.schemaConstructOptions);

    Hoek.assert(!optionValidation.error, optionValidation.error);
    options = optionValidation.value;

    this.shutdown = Lodash.once(this.shutdown);

    this.options = options;
    this.identify_host = options.identify_host;
    this.hosts = options.host;
    this.engines = {};
    this.engineList = null;
    this.enginesRegistered = false;
    this.master = null;
    this.jobs = {};
    this.running_tasks = {};
    this.net = new Net(this);
    this.log = options.log;

    // step, construct each engine
    options.engines.forEach(function (engineDef) {
        var engineLib,
            engineOpts,
            engineObj;

        engineLib = engineDef.engine;
        engineOpts = Hoek.applyToDefaults({
            log: self.log
        }, engineDef.options || {});
        engineObj = new engineLib.engine(engineOpts);

        engineObj.on('error', function (err) {
            self.log.error({engine: engineDef.name, err: err}, 'Engine error: ' + err.message);
            self.shutdown();
        });

        engineObj.on('orphaned-ack', function (task) {
            self.log.error({
                engine: engineDef.name,
                job: task.job_name
            }, 'Orphaned job tried to ack on a closed channel');
        });

        engineObj.on('orphaned-nack', function (task) {
            self.log.error({
                engine: engineDef.name,
                job: task.job_name
            }, 'Orphaned job tried to nack on a closed channel');
        });

        engineObj.on('connected', function () {
            self.log.info({
                engine: engineDef.name
            }, 'Connected');
        });

        self.engines[engineDef.name] = engineObj;
    });
    this.engineList = Object.keys(this.engines);

    // step, identify cluster and find master
    this.net.connect().then(function () {
        return self.engineList;
    }).each(function (engineKey) {
        return self.engines[engineKey].setup();
    }).then(function () {
        self.enginesRegistered = true;

        Object.keys(self.jobs).forEach(function (jobKey) {
            self.jobs[jobKey].setupSource();
        });
    });
}

util.inherits(GovernorAgent, Emitter);

internals.schemaConstructOptions = Joi.object().keys({
    identify_host: Joi.string(),
    host: Joi.array().includes(Joi.string()).single().default(null),
    engines: Joi.array().includes(Joi.object().keys({
        name: Joi.string().required(),
        engine: Joi.object().keys({
            engine: Joi.func().required()
        }),
        options: Joi.object().optional()
    })).single().required(),
    log: Joi.object().keys({
        info: Joi.func().required().label('log.info'),
        warn: Joi.func().required().label('log.warn'),
        error: Joi.func().required().label('log.error'),
        fatal: Joi.func().required().label('log.fatal'),
        debug: Joi.func().required().label('log.debug'),
        trace: Joi.func().required().label('log.trace'),
        child: Joi.func().required().label('log.child')
    })
}).without('identify_host', 'host');

GovernorAgent.prototype.consume = function GovernorAgentConsume (options) {
    Hoek.assert(options.job, 'Job must contain a job name');

    this.jobs[options.job] = new Job(options, this);
};

GovernorAgent.prototype.getClusterInfo = function () {
    var self = this;

    return this.hosts;
};

GovernorAgent.prototype.shutdown = function (reason, exitCode) {
    var killTimeout,
        killFunc,
        poleFunc,
        self = this;

    exitCode = exitCode || 0;

    killFunc = function () {
        self.log.fatal({reason: reason}, 'Shutting down');
        setImmediate(function () {
            process.exit(exitCode);
        });
    };

    killTimeout = setTimeout(killFunc, 5000);

    poleFunc = function () {
        if (Object.keys(self.running_tasks).length === 0) {
            clearTimeout(killTimeout);
            return killFunc();
        }

        setTimeout(poleFunc, 1000);
    };

    poleFunc();
};

module.exports = GovernorAgent;