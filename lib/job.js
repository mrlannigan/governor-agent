'use strict';

var Hoek = require('hoek-boom'),
    Joi = require('joi'),
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

    // if agent setup already, setup the source
    if (agent.enginesRegistered) {
        this.setupSource();
    }
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

GovernorJob.prototype.setupSource = function () {
    this.source.control = this.agent.engines[this.source.engine].consume(this.source.options, function (task, ack, nack) {
        console.log(task);
        ack();
    });
};

module.exports = GovernorJob;
