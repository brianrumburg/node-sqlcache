var _ = require('lodash');
var sql = require('mssql');
var diff = require('./diff.js');
var mc = require('./memoryCache.js');
var Emitter = require('events').EventEmitter;
var Promise = require('promise');
var inspector = require('schema-inspector');

var refresh = function(currentCache, checksumQuery) {
  var that = this;

  var result = inspector.validate(that.refreshValidationSchema, currentCache);
  if(!result.valid) {
    throw new Error(result.format());
  }

  if(!(checksumQuery || that.config.checksumQuery)) {
    throw new Error('sqlcache: checksumQuery must be passed to refresh() as second parameter, or as a property of create() options parameter.');
  }

  //build up a dictionary based on cache rows and key columns
  var cacheDict = _.reduce(currentCache, function(c, row) {
    c.set(that.diffEngine.getKeys(row), row);
    return c;
  }, mc.create());

  var checksumRequest = new sql.Request();
  checksumRequest.verbose = that.config.verbose;
  
  return checksumRequest.query(checksumQuery || that.config.checksumQuery).then(function(recordset) {

    //get diffs between current cache and new checksum recordset
    var diffs = that.diffEngine.diff(cacheDict, recordset);

    return Promise.all(_.union(

      _.map(diffs.inserts, function(r) {
        var singleRequest = new sql.Request();
        singleRequest.verbose = that.config.verbose;

        return _.reduce(that.config.keyColumns, function(req, k) {
          return req.input(k, r[k]);
        }, singleRequest)
          .query(that.config.singleQuery)
          .then(function(recordset) {
            return Promise.resolve(that.emit('insert', _.first(recordset)));
          });
      }),

      _.map(diffs.updates, function(r) {
        var singleRequest = new sql.Request();
        singleRequest.verbose = that.config.verbose;

        return _.reduce(that.config.keyColumns, function(req, k) {
          return req.input(k, r[k]);
        }, singleRequest)
          .query(that.config.singleQuery)
          .then(function(recordset) {
            return Promise.resolve(that.emit('update', _.first(recordset)));
          });
      }),

      _.map(diffs.deletes, function(r) {
        return Promise.resolve(that.emit('delete', r));
      })
        
    ));
    
  }).catch(function(err) {
    console.log('sqlcache: error during refresh.  reconnecting and trying again after timeout.');
    
    setTimeout(function() {
      connect(that).then(function() {
        refresh(currentCache, checksumQuery);
      });
    }, that.config.connectRetryTimeout);

    throw err;
  });
};

var connect = function(em) {
  console.log('sqlcache: connecting to', em.config.connectionString, '...');

  return sql.connect(em.config.connectionString).then(function() {
    console.log('sqlcache: connected to', em.config.connectionString);
    em.emit('ready');
  }).catch(function(error) {
    console.log('sqlcache: connection error', error.stack, 'reconnect in', em.config.connectRetryTimeout, 'ms');
    setTimeout(function() { connect(em) }, em.config.connectRetryTimeout);
  });
};

var create = function(options) {
  var em = new Emitter();
  em.config = {
    connectRetryTimeout: 10000,
    verbose: false
  };
  _.merge(em.config, options);

  em.diffEngine = diff.create(em.config);

  //build validation schema so later we can make sure we are sent an array of objects with all the proper key columns
  em.refreshValidationSchema = {
    type: 'array',
    items: {
      type: 'object',
      properties: _.reduce(_.union(options.keyColumns, [options.checksumColumn]), function(acc, key) {
        acc[key] = { type: 'any' };
        return acc;
      }, {})
    }
  };

  connect(em);

  em.refresh = refresh.bind(em);

  return em;
};

module.exports.create = create;
