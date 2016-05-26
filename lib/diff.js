var _ = require('lodash');
var util = require('./util.js');
var Emitter = require('events').EventEmitter;

var create = function(options) {
  var config = {
    keyColumns: [],
    checksumColumn: ""
  };
  
  _.merge(config, options);

  var em = new Emitter();

  em.initCache = function(cache, recordset) {
    //store each record in the cache by concatenated primary key
    _.forEach(recordset, function(record) {
      cache[util.buildConcatenatedKey(config.keyColumns, record)] = record;
    });
  };

  em.diffCache = function(oldCache, recordset) {
    //compare the recordset to our cache
    var newCache = _.reduce(recordset, function(dict, record) {

      var dictKey = util.buildConcatenatedKey(config.keyColumns, record);

      //if it isn't in the cache, it is an insert
      var cacheEntry = oldCache[dictKey];
      if(!cacheEntry) {
        em.emit('insert', record);
      } else { //we have an entry
        //if checksum is different, it is an update
        if(cacheEntry[config.checksumColumn] != record[config.checksumColumn]) {
          em.emit('update', record);
        }

        //remove records that are present in new recordset from prior cache
        //records remaining in previous cache will be considered deletes
        _.unset(oldCache, dictKey);
      }
      dict[dictKey] = record;
      return dict;
    }, {});

    _.forEach(oldCache, function(cacheEntry) {
      em.emit('delete', cacheEntry);
    });

    return newCache;
  }

  return em;
}

module.exports.create = create;
