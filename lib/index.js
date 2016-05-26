var _ = require('lodash');
var sql = require('mssql');
var util = require('./util.js');

var config = {
  host: 'localhost',
  database: 'sqlcache',
  username: 'username',
  password: 'password'
}

var cache = {};

var init = function(options) {
  _.merge(config, options);
  //console.log('sqlcache init...');

  sql.connect(`mssql://${config.username}:${config.password}@${config.host}/${config.database}`).then(function() {
    //console.log('connected');
    // Query
  	new sql.Request().query(config.checksumQuery).then(function(recordset) {
  		//console.dir(recordset);
      //store each record in the cache by concatenating values of the key columns
      _.reduce(recordset, function(dict, record) {
        //store the record in the dictionary using the concatenated key
        dict[util.buildConcatenatedKey(config.keyColumns, record)] = record;
        return dict;
      }, cache);

      console.log(cache);
  	}).catch(function(err) {
  		// ... query error checks
      console.log('query error', err);
  	});

  }).catch(function(err) {
  	// ... connect error checks
    console.log('connection error', err);
  });
};

var refresh = function() {
  console.log('refresh');
  new sql.Request().query(config.checksumQuery).then(function(recordset) {

    //compare the recordset to our cache
    var newCache = _.reduce(recordset, function(dict, record) {
      
      var dictKey = util.buildConcatenatedKey(config.keyColumns, record);

      //if it isn't in the cache, it is an insert
      var cacheEntry = cache[dictKey];
      if(!cacheEntry) {
        console.log('insert', record);
      } else { //we have an entry
        //if checksum is different, it is an update
        if(cacheEntry[config.checksumColumn] != record[config.checksumColumn]) {
          console.log('update', record);
        }

        //remove records that are present in new recordset from prior cache
        //records remaining in previous cache will be considered deletes
        _.unset(cache, dictKey);
      }
      dict[dictKey] = record;
      return dict;
    }, {});

    _.forEach(cache, function(cacheEntry) {
      console.log('delete', cacheEntry);
    })

    //replace old cache with newly reduced recordset
    cache = newCache;

  }).catch(function(err) {
    // ... query error checks
    console.log('query error', err);
  });
};

var close = function() {
  sql.close();
};

module.exports.init = init;
module.exports.refresh = refresh;
module.exports.close = close;
