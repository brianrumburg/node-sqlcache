var _ = require('lodash');

var buildConcatenatedKey = function(keyColumns, record) {
  return _.reduce(keyColumns, function(key, col) {
    //get the value for the column from the record
    var keyVal = record[col].toString();

    //if it is the first column in the accumulator, return the value
    //otherwise, concatenate it with the result of previous key column values
    return key ? `${key}|${keyVal}` : keyVal;
  }, "");
};

module.exports.buildConcatenatedKey = buildConcatenatedKey;
