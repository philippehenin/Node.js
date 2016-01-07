'use strict';

var kafka = require('..');
var couchbase = require("couchbase");
var config = require("./config");
var argv = require('optimist').argv;
var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;
var topic = argv.topic || config.kafka.topic || 'topic1';
var client = new Client(config.kafka.server);
var topics = [ { topic: topic }];
var options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };
var consumer = new HighLevelConsumer(client, topics, options);
var bucket = (new couchbase.Cluster(config.couchbase.server)).openBucket(config.couchbase.bucket);

consumer.on('message', function (message) {
    var buffer = JSON.parse(message.value)
    //console.log(" test buffer -> "+ buffer )
    //console.log(" test buffer.key  -> "+ buffer.key )
     bucket.insert(buffer.key, buffer, function(error, result) {
        if (error) {
          console.log('Failed to save to Couchbase', error);
        } else {
          console.log(buffer.key);
        }
    });
});

consumer.on('error', function (err) {
    console.log('error', err);
});
