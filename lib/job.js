'use strict';

var Hoek = require('hoek-boom'),
    Joi = require('joi'),
    BPromise = require('bluebird'),
    Measured = require('measured'),
    internals = {};

function GovernorJob (options, agent) {
    var optionValidation;

    optionValidation = Joi.validate(options, internals.schemaConstructOptions);

    Hoek.assert(!optionValidation.error, optionValidation.error);
    options = optionValidation.value;

    Hoek.assert(agent.engineList.indexOf(options.source.engine) !== -1, 'Engine ' + options.source.engine + 'must already be registered');

    this.name = options.job;
    this.source = options.source;
    this.inquire = options.inquire;
    this.worker = options.worker;
    this.agent = agent;

    this.slowTime = options.slowTime || 60000;
    this.delayTime = 50;
    this.nacks = new Measured.Meter();

    // push job name to source options
    if (!this.source.options) {
        this.source.options = {};
    }
    this.source.options.job_name = this.name;

    // if agent setup already, setup the source
    if (agent.enginesRegistered) {
        this.setupSource();
    }

    this.monitorNacks = this.monitorNacks.bind(this);
    setTimeout(this.monitorNacks, 500);
}

internals.schemaConstructOptions = Joi.object().keys({
    job: Joi.string().required(),

    source: Joi.object().keys({
        engine: Joi.string().required(),
        options: Joi.object().optional()
    }).required(),

    inquire: Joi.object().keys({
        locking: Joi.boolean().required(),
        key: Joi.func().required()
    }).optional().default(null),

    worker: Joi.func()
});

GovernorJob.prototype.monitorNacks = function () {
    var newDelay = 50,
        timing = this.nacks.toJSON();

    if (timing['1MinuteRate'] > 100) {
        newDelay = 10000;
    } else if (timing['1MinuteRate'] > 10) {
        newDelay = 1000;
    } else {
        newDelay = 50;
    }

    if (this.delayTime !== newDelay) {
        this.agent.log.warn({
            job: this.name,
            newDelay: newDelay,
            oldDelay: this.delayTime,
            minuteRate: timing['1MinuteRate']
        }, 'Changing nack delay time');
    }

    this.delayTime = newDelay;

    setTimeout(this.monitorNacks, 1000);
};

GovernorJob.prototype.registerJob = function () {
    return this.agent.net.registerJob(this.name, this.agent.id);
};

GovernorJob.prototype.setupSource = function () {
    var self = this;

    this.source.control = this.agent.engines[this.source.engine].consume(this.source.options, function (task, ack, nack) {

        self.handleTask(task, ack, function () {
            self.nacks.mark();

            if (self.delayTime > 1) {
                setTimeout(nack, self.delayTime);
            } else {
                nack();
            }
        });
    });
};

GovernorJob.prototype.handleTask = function (task, ack, nack) {
    var self = this,
        log = self.agent.log.child({job: this.name}),
        governor,
        slowTimeout,
        jobId,
        generatedLocks = [];

    governor = {
        done: function () {
            clearTimeout(slowTimeout);
            ack();
            self.agent.net.endJob(jobId, generatedLocks);
        },
        error: function (err) {
            clearTimeout(slowTimeout);
            nack();
            self.agent.net.endJob(jobId, generatedLocks);
            log.error({err: err}, 'Task erred');
        },
        log: log
    };

    return BPromise.try(function () {

        if (self.inquire) {

            generatedLocks = self.inquire.key(task);

            if (!Array.isArray(generatedLocks)) {
                if (typeof generatedLocks !== 'string') {
                    throw new Error('Invalid response from locking key generation');
                } else {
                    generatedLocks = [generatedLocks];
                }
            }

            generatedLocks = generatedLocks.map(function (lock) {
                return {
                    key: lock,
                    locking: self.inquire.locking
                }
            });
        }


        return self.agent.net.inquireLocks(generatedLocks, self.name, self.agent.id).then(function (status) {
            if (!status.ok) {
                nack();
                log.debug({lock: status}, 'Locks not ok');
                return;
            }

            jobId = status.id;

            slowTimeout = setTimeout(function () {
                log.warn('Job is slow');
            }, self.slowTime);

            return self.worker(task).then(function () {
                governor.done();
            }).catch(function (err) {
                governor.error(err);
            });
        });
    }).catch(function (err) {
        self.agent.log.error({err: err, job: self.name}, 'Task failed');
        nack();
    });
};

module.exports = GovernorJob;
