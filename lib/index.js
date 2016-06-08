var _ = require('lodash');
var sql = require('mssql');
var diff = require('./diff.js');
var mc = require('./memoryCache.js');
var Emitter = require('events').EventEmitter;

var refresh = function(currentCache) {
  var that = this;

  //build up a dictionary based on cache rows and key columns
  var cacheDict = _.reduce(currentCache, function(c, row) {
    c.set(that.diffEngine.getKeys(row), row);
    return c;
  }, mc.create());

  new sql.Request().query(this.config.checksumQuery).then(function(recordset) {

    //replace old cache with newly reduced recordset
    var diffs = that.diffEngine.diff(cacheDict, recordset);

    _.forEach(diffs.inserts, function(r) {
      var req = new sql.Request();

      _.forEach(that.config.keyColumns, function(k) {
        req.input(k, sql.Int, r[k]);
      });

      req.query(that.config.singleQuery).then(function(recordset) {
        that.emit('insert', _.first(recordset));
      }).catch(function(error) {
        console.log('sqlcache: single query error', err.stack);
      });
    });

    _.forEach(diffs.updates, function(r) {
      var req = new sql.Request();

      _.forEach(that.config.keyColumns, function(k) {
        req.input(k, sql.Int, r[k]);
      });

      req.query(that.config.singleQuery).then(function(recordset) {
        that.emit('update', _.first(recordset));
      }).catch(function(error) {
        console.log('sqlcache: single query error', err.stack);
      });
    });

    _.forEach(diffs.deletes, function(r) {
      that.emit('delete', r);
    });
  }).catch(function(err) {
    console.log('sqlcache: query error', err.stack);
    that.emit('query error', err.stack);
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
  }).catch(function(err) {
    console.log('sqlcache: connection error', err.stack);
    em.emit('connection error', err.stack);
  });

  em.refresh = refresh.bind(em);

  return em;
};

module.exports.create = create;
