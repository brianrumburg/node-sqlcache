var _ = require('lodash');
var sql = require('mssql');
var diff = require('./diff.js');
var mc = require('./memoryCache.js');
var Emitter = require('events').EventEmitter;
var Promise = require('promise');

var refresh = function(currentCache) {
  var that = this;

  //build up a dictionary based on cache rows and key columns
  var cacheDict = _.reduce(currentCache, function(c, row) {
    c.set(that.diffEngine.getKeys(row), row);
    return c;
  }, mc.create());

  var checksumRequest = new sql.Request();
  
  return checksumRequest.query(this.config.checksumQuery).then(function(recordset) {

    //get diffs between current cache and new checksum recordset
    var diffs = that.diffEngine.diff(cacheDict, recordset);

    return Promise.all(_.union(

      _.map(diffs.inserts, function(r) {
        return _.reduce(that.config.keyColumns, function(req, k) {
          return req.input(k, sql.Int, r[k]);
        }, new sql.Request())
          .query(that.config.singleQuery)
          .then(function(recordset) {
            return Promise.resolve(that.emit('insert', _.first(recordset)));
          });
      }),

      _.map(diffs.updates, function(r) {
        return _.reduce(that.config.keyColumns, function(req, k) {
          return req.input(k, sql.Int, r[k]);
        }, new sql.Request())
          .query(that.config.singleQuery)
          .then(function(recordset) {
            return Promise.resolve(that.emit('update', _.first(recordset)));
          });
      }),

      _.map(diffs.deletes, function(r) {
        return Promise.resolve(that.emit('delete', r));
      })
        
    ));
    
  });
};

var close = function() {
  sql.close();
};

var create = function(options) {
  var em = new Emitter();
  em.config = {}
  _.merge(em.config, options);

  em.diffEngine = diff.create(em.config);

  console.log('sqlcache: connecting to', em.config.connectionString, '...');

  sql.connect(em.config.connectionString).then(function() {
    console.log('sqlcache: connected to', em.config.connectionString);
    em.emit('ready');
  }).catch(function(error) {
    console.log('sqlcache: connection error', err.stack);
    throw new Error(error);
  });

  em.refresh = refresh.bind(em);

  return em;
};

module.exports.create = create;
