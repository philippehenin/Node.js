'use strict';
// utiliser le fichier de config ./config.json
// utilise kafka-node
// git clone https://github.com/SOHU-Co/kafka-node.git
// mettre le js dans kafka-node/example
// todo -> fichier pour les dependences
var kafka = require('kafka-node');
var couchbase = require("couchbase");
var config = require("./config");
var HighLevelConsumer = kafka.HighLevelConsumer;
var Client = kafka.Client;
var topic = config.kafka.topic || 'topic1';
var client = new Client(config.kafka.server);
var topics = [ { topic: topic }];
var options = { autoCommit: true, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024*1024 };
var consumer = new HighLevelConsumer(client, topics, options);
var bucket = (new couchbase.Cluster(config.couchbase.server)).openBucket(config.couchbase.bucket);

console.log("*************************************************************************************")
console.log("* Source : kafka                                                                     ")
console.log("* Source : server :" + config.kafka.server)
console.log("* Source : topic  :" + config.kafka.topic)
console.log("* Cible  : couchbase                                                                 ")
console.log("* Cible  : server :" + config.couchbase.server)
console.log("* Cible  : bucket :" + config.couchbase.bucket)
console.log("*************************************************************************************")
console.log("* LISTENING                                                                         *")
console.log("*************************************************************************************")

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
