var _ = require('lodash');

var create = function(options) {
  var engine = {
    config: {
      keyColumns: [],
      checksumColumn: ""
    }
  };

  _.merge(engine.config, options);

  engine.getKeys = function(record) {
    return _.reduce(engine.config.keyColumns, function(acc, col) {
      acc[col] = record[col];
      return acc;
    }, {});
  };

  engine.diff = function(oldCache, recordset) {
    //var newCache = _.reduce(recordset, function(record) {}, )

    var diffs = {
      inserts: [],
      updates: [],
      deletes: []
    };

    //compare the recordset to our cache
    _.forEach(recordset, function(record) {
      //if it isn't in the cache, it is an insert
      var cacheEntry = oldCache.get(engine.getKeys(record));
      if(!cacheEntry) {
        diffs.inserts.push(record);
      } else { //we have an entry
        //if checksum is different, it is an update
        if(cacheEntry[engine.config.checksumColumn] != record[engine.config.checksumColumn]) {
          diffs.updates.push(record);
        }

        //remove records that are present in new recordset from prior cache
        //records remaining in previous cache will be considered deletes
        oldCache.unset(engine.getKeys(record));
      }
    });

    _.forEach(oldCache.getAll(), function(record) {
      diffs.deletes.push(record);
    });

    return diffs;
  }

  return engine;
}

module.exports.create = create;
