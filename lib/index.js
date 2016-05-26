var _ = require('lodash');
var sql = require('mssql');
var diff = require('./diff.js');
var mc = require('./memoryCache.js');

var config = {
  host: 'localhost',
  database: 'sqlcache',
  username: 'username',
  password: 'password'
}

var cache = mc.create();

var diffEngine = null;

var init = function(options) {
  _.merge(config, options);

  diffEngine = diff.create(config);

  sql.connect(`mssql://${config.username}:${config.password}@${config.host}/${config.database}`).then(function() {

  	new sql.Request().query(config.checksumQuery).then(function(recordset) {
      diffEngine.initCache(cache, recordset, config.keyColumns);
      console.log('initial cache\n', JSON.stringify(cache.getAll(), null, 2));
  	}).catch(function(err) {
      console.log('query error', err);
  	});

  }).catch(function(err) {
    console.log('connection error', err);
  });

  diffEngine.on('insert', (r) => console.log('insert', r));
  diffEngine.on('update', (r) => console.log('update', r));
  diffEngine.on('delete', (r) => console.log('delete', r));
};

var refresh = function() {
  console.log('refresh');
  new sql.Request().query(config.checksumQuery).then(function(recordset) {

    //replace old cache with newly reduced recordset
    var newCache = mc.create();
    diffEngine.diffCache(cache, newCache, recordset);
    cache = newCache;

  }).catch(function(err) {
    console.log('query error', err);
  });
};

var close = function() {
  sql.close();
};

module.exports.init = init;
module.exports.refresh = refresh;
module.exports.close = close;
