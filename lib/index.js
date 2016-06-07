var _ = require('lodash');
var sql = require('mssql');
var diff = require('./diff.js');
var mc = require('./memoryCache.js');
var Emitter = require('events').EventEmitter;

var refresh = function(currentCache) {
  debugger;
  //console.log('refresh. current cache:\n', currentCache);
  var that = this;
  //build up a dictionary based on cache rows and key columns
  var cacheDict = _.reduce(currentCache, function(c, row) {
    c.set(that.diffEngine.getKeys(row), row);
    return c;
  }, mc.create());

  //console.log('cacheDict:\n', cacheDict.getAll());

  new sql.Request().query(this.config.checksumQuery).then(function(recordset) {

    //replace old cache with newly reduced recordset
    var diffs = that.diffEngine.diff(cacheDict, recordset);
    //console.log('diffs:\n', JSON.stringify(diffs, null, 2));

    _.forEach(diffs.inserts, function(r) {
      debugger;
      //console.log('insert!!', r);
      var req = new sql.Request();

      _.forEach(that.config.keyColumns, function(k) {
        req.input(k, sql.Int, r[k]);
      });

      req.query(that.config.singleQuery).then(function(recordset) {
        debugger;
        //console.log('insert singleQuery returned!', recordset);
        that.emit('insert', _.first(recordset));
      }).catch(function(error) {
        console.log('single query error', err.stack);
      });
    });

    _.forEach(diffs.updates, function(r) {
      debugger;
      //console.log('update!!', r);
      var req = new sql.Request();

      _.forEach(that.config.keyColumns, function(k) {
        req.input(k, sql.Int, r[k]);
      });

      req.query(that.config.singleQuery).then(function(recordset) {
        debugger;
        //console.log('update singleQuery returned!', recordset);
        that.emit('update', _.first(recordset));
      }).catch(function(error) {
        console.log('single query error', err.stack);
      });
    });

    _.forEach(diffs.deletes, function(r) {
      debugger;
      //console.log('delete!!', r);
      that.emit('delete', r);
    });
  }).catch(function(err) {
    console.log('query error', err.stack);
    that.emit('query error', err.stack);
  });
};

var close = function() {
  sql.close();
};

var create = function(options) {
  var em = new Emitter();
  em.config = {
    host: 'localhost',
    database: 'sqlcache',
    username: 'username',
    password: 'password'
  }

  _.merge(em.config, options);

  em.diffEngine = diff.create(em.config);

  sql.connect('mssql://' + em.config.username + ':' + em.config.password +
    '@' + em.config.host + '/' + em.config.database).then(function() {
    em.emit('ready');
  }).catch(function(err) {
    console.log('connection error', err.stack);
    em.emit('connection error', err.stack);
  });

  em.refresh = refresh.bind(em);

  return em;
};

module.exports.create = create;
