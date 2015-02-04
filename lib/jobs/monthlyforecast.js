'use strict';

var config = require('web-config').getConfig(),
    datafactory = require('web-datafactory/generator').setConfig(config),
    BPromise = require('bluebird'),
    forecasting = require('svc-loco-forecasting'),
    els = datafactory.elasticsearch('loco'),
    index = 'loco',
    type = 'campaigns',
    monthlyforecast = {};


/**
 *
 * @param task {object}
 * @param task.zip_code {string}
 */
monthlyforecast.worker = function (task) {
    var self = this,
        ok = this.reporting(task.zip_code);

    ok = ok.then(function () {
        return BPromise.resolve(els.search({
            index: index,
            type: type,
            q: 'zip_code:95629'
        }));

    });

    ok = ok.then(function (campaignsData) {
        var campaigns = campaignsData.hits.hits,
            location = {
                campaign: {}
            }; // we should clone from a location object template or something like that

        campaigns.forEach(function (campaign) {
            campaign._source.hourly = forecasting.hourlyForecast(campaign._source.inventory.total);
        });

        location.campaign.count = campaigns.length;
        location.campaign.hourly_impression_count = self.sumhourlyimpressions(campaigns.map(function (campaign) {
            return campaign.hourly;
        }));
        // calculate daily impressions
        // calculate hourly impressions for next day
        // sum hourly impressions of all campaigns for hourly impression plan
        // insert/update/replace/whatever location object into elasticsearch
        // end job

        return campaigns;
    });

    return ok;


};

/**
 *
 * @param campaigns
 * @returns {array}
 */
monthlyforecast.sumhourlyimpressions = function sumhourlyimpressions(hourlyimpressions) {

    return hourlyimpressions.reduce(function (prev, curr) {
        if (!prev) {
            return curr;
        }

        return curr.map(function (val, index) {
            return val + prev[index];
        });

    }, null);
};


module.exports = monthlyforecast;

/**
 *
 * @param task {object}
 * @param task.zip_code {string}
 */
monthlyforecast.inquire = function (zipcode) {
    // return locks
};

/**
 *
 * @param zipcode {string}
 * @returns {promise}
 */
monthlyforecast.reporting = function (zipcode) {
    // do reporting - impressions compared to projected per day
    // per campaign impressions projected per day
    // total impressions compared to projected
    // per campaign impressions compared to projected
    // number of campaigns
    return BPromise.resolve();
    // store it in elastic search?  mongo?  store it nowhere for now
};
