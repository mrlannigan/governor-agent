'use strict';

var Joi = require('joi'),
    Hoek = require('hoek-boom'),
    Job = require('./job'),
    Net = require('./net'),
    util = require('util'),
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
        self = this,
        i,
        il,
        engineDef,
        engineLib,
        engineOpts;

    optionValidation = Joi.validate(options, internals.schemaConstructOptions);

    Hoek.assert(!optionValidation.error, optionValidation.error);
    options = optionValidation.value;

    this.options = options;
    this.identify_host = options.identify_host;
    this.hosts = options.host;
    this.engines = {};
    this.engineList = null;
    this.enginesRegistered = false;
    this.master = null;
    this.jobs = {};
    this.net = new Net(this);
    this.log = options.log;

    // step, construct each engine
    for (i = 0, il = options.engines.length; i < il; i++) {
        engineDef = options.engines[i];
        engineLib = engineDef.engine;
        engineOpts = Hoek.applyToDefaults({
            log: this.log
        }, engineDef.options || {});
        this.engines[engineDef.name] = new engineLib.engine(engineOpts);
    }
    this.engineList = Object.keys(this.engines);

    // step, identify cluster and find master
    this.net.connect().then(function () {
        return self.engineList;
    }).each(function (engineKey) {
        return self.engines[engineKey].setup();
    }).then(function () {
        self.enginesRegistered = true;

        self.jobs.somesweetname.setupSource();
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

module.exports = GovernorAgent;
