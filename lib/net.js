'use strict';

var SocketClient = require('socket.io-client'),
    Wreck = require('wreck'),
    BPromise = require('bluebird'),
    Hoek = require('hoek-boom'),
    util = require('util');

function GovernorAgentNet (agent) {
    this.agent = agent;
    this.queue = [];
    this.NAMESPACE_AGENT = '/agent';
    this.connection = null;
}

GovernorAgentNet.prototype.connect = function (options) {
    var agent = this.agent,
        self = this,
        defaults = {autoConnect: false, reconnection: false, forceNew: true},
        config = Hoek.applyToDefaults(defaults, options || {}),
        connection,
        identifyHost,
        reconnectProm,
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

    // connect and identify the governor cluster
    ok = new BPromise(function (resolve, reject) {

        Wreck.get(util.format('http://%s/api/nodes', identifyHost), {
            timeout: 5000,
            json: true
        }, function (err, res, payload) {
            if (err) {
                return reconnectProm(err.toString(), 5000, {err: err});
            }

            if (!payload || payload.length < 1 || Array.isArray(payload) == false) {
                var error = new Error('Invalid response');
                error.host = identifyHost;
                error.payload = payload;
                return reconnectProm(error.toString(), 5000, {err: error});
            }

            self.agent.hosts = payload;

            resolve(payload);
        });

    });

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

        connection = SocketClient(url, config);

        connection.on('connect', function () {
            agent.log.info({event: 'connect', node: node}, 'connected to governor');
        });

        connection.on('connect_error', function (err) {
            reconnectProm('connection to governor erred', 5000, {event: 'connect_error', err: err, node: node});
        });

        connection.on('error', function (err) {
            reconnectProm('governor connection error', 5000, {event: 'error', err: err, node: node});
        });

        connection.on('disconnect', function () {
            self.connection.close();
            self.connection = null;
            reconnectProm('governor connection closed', 5000, {event: 'disconnect', node: node});
        });

        connection.open();

        self.connection = connection;

        return connection;
    });

    return ok;
};

module.exports = GovernorAgentNet;