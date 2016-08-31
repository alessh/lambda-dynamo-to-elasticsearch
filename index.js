// dependencies
var aws = require('aws-sdk');
var _ = require('underscore');
var path = require('path');
var elasticsearch = require('elasticsearch');
var http_aws_es = require('http-aws-es');
var when = require('when');
var moment = require('moment');
var table = 'altamira';
var region = 'us-east-1';
var es_domain = 'altamira';
var es_endpoint = 'https://search-altamira-vsxf6dgsuddwa4wcebb45u5wge.us-east-1.es.amazonaws.com';

exports.handler = function(event, context, callback) {


  //  Log out the entire invocation
  // console.log(JSON.stringify(event, null, 2));
  var aws_es = new aws.ES({
    region: region
  });

  // Promise - Describe the ES Domain in order to get the endpoint url
  when.promise(function(resolve, reject, notify){
    // aws_es.describeElasticsearchDomains({
    //   DomainNames: [ es_domain ]
    // }, function(err, data){
    //   if (err) {
    //     console.log("describeElasticsearchDomains error:", err, data);
    //     reject(err);
    //   } else {
    //     resolve({domain: _.first(data.DomainStatusList)});
    //   }
    // });
    resolve({
      domain: {
        Endpoint: es_endpoint
      }
    });
  }).then(function(result){
    // Promise - Create the Index if needed - resolve if it is already there
    // or create and then resolve
    var promise = when.promise(function(resolve, reject, notify){
      var myCredentials = new aws.EnvironmentCredentials('AWS');
      var es = elasticsearch.Client({
        hosts: result.domain.Endpoint,
        connectionClass: http_aws_es,
        amazonES: {
          region: region,
          credentials: myCredentials
        }

      });

      es.indices.exists({
        index: table
      },function(err, response, status){
        console.log('Looking for Index');
        console.log(err, response, status);
        if (status == 200) {
          console.log('Index Exists');
          resolve({es: es, domain: result.domain});
        } else if (status == 404) {
          createIndex(es, table, function(){
            resolve({es: es, domain: result.domain});
          });
        } else {
          reject(err);
        }
      });
    });
    return promise;
  }).then(function(result){
    console.log('Index is ready');
    // Create promises for every record that needs to be processed
    // resolve as each successful callback comes in
    console.log(event.Records);
    var records = _.map(event.Records, function(record, index, all_records){
      return when.promise(function(resolve, reject, notify){
        if (record.eventName == 'REMOVE') {
            recordExists(result.es, table, record).then(function(exists){
            // Now delete the record.
            if (exists) {
              return delRecord(result.es, table, record);
            } else {
              resolve(false);
            }
          }).then(function(record){
            resolve(record);
          }, function(reason){
            console.log(reason);
            reject(reason);
          });
        } else {
          // First get the record
          recordExists(result.es, table, record).then(function(exists){
            // Now create or update the record.
            return putRecord(result.es, table, record, exists);
          }).then(function(record){
            resolve(record);
          }, function(reason){
            console.log(reason);
            reject(reason);
          });
        }
      });
    });

    // return a promise array of all records
    return when.all(records);
  }).done(function(records){
    // Succeed the context if all records have been created / updated
    console.log("Processed all records");
    //context.succeed() is deprecated with Node.js 4.3 runtime
    //context.succeed("Successfully processed " + records.length + " records.");
    callback(null, "Successfully processed " + records.length + " records.");
  }, function(reason){
    //context.fail() is deprecated with Node.js 4.3 runtime
    //context.fail("Failed to process records " + reason);
    callback("Failed to process records " + reason);
  });


};

var createIndex = function(es, table, callback) {
  console.log('createIndex', table);
  es.indices.create({
    index: table
  }, function(err, response, status){
    if (err) {
      console.log("Index could not be created", err, response, status);
    } else {
      console.log("Index has been created");
    }
  });

};

var recordExists = function(es, table, record) {
  return when.promise(function(resolve, reject, notify){

    var params = {
      index: table,
      id: _.isUndefined(record.dynamodb.NewImage) ? record.dynamodb.OldImage.id.S : record.dynamodb.NewImage.id.S,
      type: _.isUndefined(record.dynamodb.NewImage) ? record.dynamodb.OldImage.type.S : record.dynamodb.NewImage.type.S
    }

    es.get(params, function(err, response, status){
      if (status == 200) {
        console.log('Document Exists');
        resolve(true);
      } else if (status == 404) {
        console.log('Document not Exists');
        resolve(false);
      } else {
        reject(err);
      }
    });
  });
};

var putRecord = function(es, table, record, exists) {
  console.log('putRecord:', record.dynamodb.NewImage.id.S);
  return when.promise(function(resolve, reject, notify){

    var params = {
      index: table,
      id: record.dynamodb.NewImage.id.S,
      body: esBody(record),
      type: record.dynamodb.NewImage.type.S
    };
    var handler = function(err, response, status) {
      if (status == 200 || status == 201) {
        console.log('Document written');
        resolve(record);
      } else {
        console.log(err, response, status);
        reject(err);
      }
    };

    if (exists) {
      params.body = {
        doc: esBody(record)
      };
      es.update(params, handler);
    } else {
      params.body = esBody(record);
      es.create(params, handler);
    }

  });
};

var delRecord = function(es, table, record) {
  console.log('delRecord:', record.dynamodb.OldImage.id.S);
  return when.promise(function(resolve, reject, notify){

    var params = {
      index: table,
      id: record.dynamodb.OldImage.id.S,
      type: record.dynamodb.OldImage.type.S
    };
    var handler = function(err, response, status) {
      if (status == 200 || status == 201) {
        console.log('Document removed');
        resolve(record);
      } else {
        console.log('delRecord:'+err, response, status);
        reject(err);
      }
    };

    es.delete(params, handler);

  });
};

// Deal with Floats and Nulls coming in on the Stream
// in order to created a valid ES Body
// Otherwise just reformat into ES body
var esBody = function(record){
  var values = record.dynamodb.NewImage;
  var body = _.mapObject(values, function(val, key){
    var tuple = _.pairs(val)[0];
    var new_val = tuple[1];
    switch (tuple[0]) {
      case 'N':
      new_val = parseFloat(tuple[1]);
      break;
      case 'NULL':
      new_val = null;
      break;
    }
    return new_val;
  });
  return body;
};
