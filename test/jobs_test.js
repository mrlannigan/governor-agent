'use strict';

var Lab = require('lab'),
    lab = exports.lab = Lab.script(),
    describe = lab.experiment,
    it = lab.test,
    should = require('should'),
    mf = require('../lib/jobs/monthlyforecast');

describe('jobs', function () {

    describe('monthly forecast', function () {

        it('should do something', function (done) {
            mf.worker('95629')
                .then(function (data) {
                    console.log('data!', data);
                }).done(done, done);
        });

        it('should sum hourly impressions', function (done) {
            mf.sumhourlyimpressions([[1, 2, 3], [2, 3, 4], [3, 4, 5]]).should.eql([6, 9, 12]);
            done();
        });

    });

});