var _ = require('lodash');
var Emitter = require('events').EventEmitter;

var create = function(options) {
  var config = {
    keyColumns: [],
    checksumColumn: ""
  };

  var getKeys = function(record) {
    return _.reduce(config.keyColumns, function(acc, col) {
      acc[col] = record[col];
      return acc;
    }, {});
  };

  _.merge(config, options);

  var em = new Emitter();

  em.initCache = function(cache, recordset) {
    //store each record in the cache by concatenated primary key
    _.forEach(recordset, function(record) {
      cache.set(getKeys(record), record);
    });
  };

  em.diffCache = function(oldCache, newCache, recordset) {
    //compare the recordset to our cache

    _.forEach(recordset, function(record) {
      //if it isn't in the cache, it is an insert
      var cacheEntry = oldCache.get(getKeys(record));
      if(!cacheEntry) {
        em.emit('insert', record);
      } else { //we have an entry
        //if checksum is different, it is an update
        if(cacheEntry[config.checksumColumn] != record[config.checksumColumn]) {
          em.emit('update', record);
        }

        //remove records that are present in new recordset from prior cache
        //records remaining in previous cache will be considered deletes
        oldCache.unset(getKeys(record));
      }
      newCache.set(getKeys(record), record);
    });

    _.forEach(oldCache.getAll(), function(cacheEntry) {
      em.emit('delete', cacheEntry);
    });

    return newCache;
  }

  return em;
}

module.exports.create = create;
