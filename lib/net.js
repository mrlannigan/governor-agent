'use strict';

var socketClientFactory = require('socket.io-client'),
    Wreck = require('wreck'),
    BPromise = require('bluebird'),
    Hoek = require('hoek-boom'),
    os = require('os'),
    util = require('util');

function GovernorAgentNet (agent) {
    this.agent = agent;
    this.queue = [];
    this.NAMESPACE_AGENT = '/agent';
    this.connection = null;
}

GovernorAgentNet.prototype.getNetworkAddress = function () {
    var nets = os.networkInterfaces(),
        result = '127.0.0.1';

    Object.keys(nets).some(function (netInterface) {
        var candidate;

        netInterface = nets[netInterface];

        candidate = netInterface.filter(function (item) {
            return item.family === 'IPv4' && !item.internal;
        }).shift();

        if (candidate) {
            result = candidate.address;
            return true;
        }
    });

    return result;
};

GovernorAgentNet.prototype.connect = function (options) {
    var agent = this.agent,
        self = this,
        defaults = {autoConnect: false, reconnection: false, forceNew: true},
        config = Hoek.applyToDefaults(defaults, options || {}),
        connection,
        identifyHost,
        reconnectProm,
        getNodesProm,
        ok;

    // identify who's API to connect to
    identifyHost = agent.identify_host || (agent.host ? agent.host[0] : null);
    if (identifyHost != null && typeof identifyHost !== 'string' && identifyHost.hasOwnProperty('host') && identifyHost.hasOwnProperty('port')) {
        identifyHost = util.format('%s:%s', identifyHost.host, identifyHost.port);
    }

    // close any existing connection
    if (self.connection != null && self.connection.close) {
        agent.log.warn('closing existing connection');
        self.connection.close();
    }

    // gc
    self.connection = null;

    // create reconnect prom func
    reconnectProm = function (msg, delay, logObj) {
        delay = delay || 5000;
        logObj = logObj || {};

        return BPromise.resolve()
            .then(function () {
                agent.log.warn(logObj, msg);
            })
            .delay(delay)
            .then(function () {
                return self.connect(options);

            });
    };

    getNodesProm = function (msg, delay, logObj, retries) {
        var maxRetries = 10,
            defaultTimeout = 5000;

        delay = delay || defaultTimeout;
        logObj = logObj || {};

        if (!retries) {
            retries = 0;
        }

        return BPromise.resolve()
            .then(function () {
                if (retries > 0) {
                    agent.log.warn(logObj, msg);
                }
            })
            .delay(delay)
            .then(function () {
                return new BPromise(function (resolve, reject) {

                    Wreck.get(util.format('http://%s/api/nodes', identifyHost), {
                        timeout: defaultTimeout,
                        json: true
                    }, function (err, res, payload) {
                        if (err) {
                            if (retries >= maxRetries) {
                                return reject(err);
                            }

                            return resolve(getNodesProm(err.toString(), defaultTimeout, {err: err}, ++retries));
                        }

                        if (!payload || payload.length < 1 || Array.isArray(payload) === false) {
                            var error = new Error('Invalid response');
                            error.host = identifyHost;
                            error.payload = payload;

                            if (retries >= maxRetries) {
                                return reject(err);
                            }
                            return getNodesProm(error.toString(), defaultTimeout, {err: error}, ++retries);
                        }

                        self.agent.hosts = payload;

                        resolve(payload);
                    });

                });
            });
    };

    // connect and identify the governor cluster
    ok = getNodesProm(null, 50);

    // identify the master
    ok = ok.filter(function (payload) {
        return payload.master;
    });

    // error check master
    ok = ok.then(function (master) {
        if (master.length < 1) {
            return reconnectProm('No masters found, waiting 5 seconds to reconnect', 5000);
        }

        return master;
    }).get(0);

    // connect to master
    ok = ok.then(function (master) {
        var url,
            node;

        if (!master) {
            return;
        }

        node = util.format('%s:%s', master.hostname, master.port);
        url = util.format('http://%s%s', node, self.NAMESPACE_AGENT);

        connection = socketClientFactory(url, config);

        connection.on('connect', function () {
            agent.log.info({event: 'connect', node: node}, 'Connected to governor');
        });

        connection.on('connect_error', function (err) {
            reconnectProm('Connection to governor erred', 5000, {event: 'connect_error', err: err, node: node});
        });

        connection.on('error', function (err) {
            reconnectProm('Governor connection error', 5000, {event: 'error', err: err, node: node});
        });

        connection.on('disconnect', function () {
            self.connection.close();
            self.connection = null;
            reconnectProm('Governor connection closed', 5000, {event: 'disconnect', node: node});
        });

        connection.open();

        // promisify the emit... always requires an ack
        connection.emitPromise = function () {
            var args = [],
                argsLength = arguments.length,
                i = 0;

            for (; i < argsLength; i++) {
                args[i] = arguments[i];
            }

            return new BPromise(function (resolve, reject) {
                args.push(function (data) {
                    var err;
                    if (data && data.isError) {
                        err = new Error(data.message);
                        return reject(err);
                    }
                    resolve(data);
                });
                connection.emit.apply(connection, args);
            });
        };

        self.connection = connection;

        return connection;
    });

    ok = ok.catch(function (err) {
        agent.log.error({err: err}, 'Shutting down from net module');
        setTimeout(process.exit.bind(process, 1), 500);
    });

    return ok;
};

GovernorAgentNet.prototype.registerAgent = function (agentName) {
    return this.connection.emitPromise('identify', agentName);
};

GovernorAgentNet.prototype.registerJob = function (jobName, agentName) {
    return this.connection.emitPromise('register-job', jobName, agentName);
};

GovernorAgentNet.prototype.inquireLocks = function (locks, jobName, agentName) {
    return this.connection.emitPromise('handle-locks', {
        agent_name: agentName,
        job_name: jobName,
        lock_data: locks
    });
};

GovernorAgentNet.prototype.endJob = function (jobId, locks) {
    return this.connection.emitPromise('job-end', {
        id: jobId,
        lock_data: locks
    });
};

module.exports = GovernorAgentNet;
