'use strict';

var assert = require('assert');
var Promise = require('promise');
var logger = require('./logger');
var config = require('./config');

var mongodb = require('mongodb');
var mongoDbQueue = require('mongodb-queue');
var dbConnect = Promise.denodeify(mongodb.MongoClient.connect);

var conn = config.conn;
var queue = null;
var deadQueue = null;

//
// returns a promise
// provides mongodb database if resolved
//
var init = function() {
	logger.debug('begin init');

	var promise = new Promise(function(resolve, reject) {
		dbConnect(conn)
			.then(function(db) {
				logger.debug('connected to mongodb');

				var queueing = config.queueing;
		    deadQueue = mongoDbQueue(db, queueing.deadQueueName);
				queue = mongoDbQueue(db, 
					queueing.queueName, 
					{ visibility : queueing.visibility, deadQueue: deadQueue, maxRetries: queueing.maxRetries }
				);
		    assert.notEqual(undefined, queue, 'queue is not defined');
		    assert.notEqual(undefined, deadQueue, 'deadQueue is not defined');
		    logger.debug('queues created');
		    resolve(db);
			})
			.catch(function(err) {
				logger.warn('initialization failed', err.stack);
				reject(err);
			});
	});

	return promise;
}

//
// process events recursively
//
var processEvent = function() {
	logger.debug('inside processEvent');

	queue.get(function(err, msg) {
		if (err) { logger.warn(err.stack); }

		if (msg.id === undefined) {
			logger.debug('no message found');
			//
			// go gack to sleep
			// 
			setTimeout(processEvent, 10000);
		} else {
			logger.debug('got a message', msg.id);

			//
			// TODO: do some meaningful work here
			//

			queue.ack(msg.ack, function(err, id) {
				if (err) { logger.warn(err.stack); }
				logger.debug('message acknowledged');

				logger.debug('recursively call processEvent');
				processEvent();
			});
		}
	});		
}

init().then(function(db) {
	console.log('init complete');
	processEvent();
});