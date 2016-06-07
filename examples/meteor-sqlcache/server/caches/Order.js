import sc from 'sqlcache';

var cache = sc.create({
  host: 'localhost',
  catalog: 'sqlcache',
  username: 'sqlcache',
  password: 'sqlcache',
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
  cache.refresh(
    Orders.find({},{
      companyId: 1,
      orderId: 1,
      checksum: 1
    }).fetch()
  );
};

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
