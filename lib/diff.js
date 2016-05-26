var _ = require('lodash');
var util = require('./util.js');

var initCache = function(cache, recordset, keyColumns) {
  //store each record in the cache by concatenated primary key
  _.forEach(recordset, function(record) {
    cache[util.buildConcatenatedKey(keyColumns, record)] = record;
  });
};

module.exports.initCache = initCache;
