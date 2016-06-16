import { Meteor } from 'meteor/meteor';
import sc from 'sqlcache';
import { Orders } from '../../collections'

var cache = sc.create({
  connectionString: Meteor.settings.connectionString,
  verbose: Meteor.settings.verbose,
  keyColumns: [ 'companyId', 'orderId' ],
  checksumColumn: 'checksum',
  checksumQuery: `
  SELECT
    companyId, orderId,
    CHECKSUM(customerId) checksum
  FROM
    [Order]
`,
  singleQuery: `
  SELECT
    companyId, orderId, customerId,
    CHECKSUM(customerId) checksum
  FROM
    [Order]
  WHERE
    companyId = @companyId
    AND orderId = @orderId
`
});

var refresh = function() {
  return cache.refresh(Orders.find({},{
    fields: {
      companyId: 1,
      orderId: 1,
      checksum: 1
    }
  }).fetch());
};

cache.on('ready', Meteor.bindEnvironment(() => {
  console.log('Order cache ready. Refreshing...');
  refresh().then(() => {
    console.log('Order cache refresh complete.');
  }).catch((err) => {
    console.log('Order cache init error:', err.stack);
  });
}));

var upsertHandler = Meteor.bindEnvironment(function(row) {
  Orders.upsert({
    companyId: row.companyId,
    orderId: row.orderId
  }, {$set: row});
});

cache.on('insert', upsertHandler);
cache.on('update', upsertHandler);

cache.on('delete', Meteor.bindEnvironment(function(row) {
  Orders.remove({
    companyId: row.companyId,
    orderId: row.orderId
  });
}));

module.exports.refresh = refresh;
