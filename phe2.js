'use strict';

var kafka = require('..');
var couchbase = require("couchbase");
var uuid = require("uuid");
var config = require("./config");

var argv = require('optimist').argv;

var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;





//console.log("kafka");
//console.log(config.kafka.topic);
//console.log(config.kafka.server);
//console.log("couchbase")
//console.log(config.couchbase.bucket);
//console.log(config.couchbase.server);

var topic = argv.topic || config.kafka.topic || 'topic1';

var client = new Client(config.kafka.server);
var topics = [ { topic: topic }];
var options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };
var consumer = new HighLevelConsumer(client, topics, options);




var bucket = (new couchbase.Cluster(config.couchbase.server)).openBucket(config.couchbase.bucket);

var jsonData = {
    id: uuid.v4(),
   username: "philippehenin",
}







consumer.on('message', function (message) {
    console.log(message);


    jsonData = {
        id: message.value.key,
        data: message.value,
    }

    console.log(" test -> "+ jsonData.id )
    bucket.insert(jsonData.id, jsonData, function(error, result) {
        if (error) {
          console.log('Failed to save to Couchbase', error);
        } else {
          console.log('Saved to Couchbase!');
        }
    });






});

consumer.on('error', function (err) {
    console.log('error', err);
});
