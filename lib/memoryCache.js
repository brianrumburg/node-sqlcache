var _ = require('lodash');

var create = function() {
  var obj = {};

  obj.cache = {};

  obj.set = function(key, record) {
    obj.cache[JSON.stringify(key)] = record;
  }

  obj.get = function(key) {
    return obj.cache[JSON.stringify(key)];
  }

  obj.getAll = function() {
    return obj.cache;
  }

  obj.unset = function(key) {
    _.unset(obj.cache, JSON.stringify(key));
  }

  return obj;
};

module.exports.create = create;
