import sc from 'sqlcache';

var orderDetailCache = sc.create({
  host: 'localhost',
  catalog: 'sqlcache',
  username: 'sqlcache',
  password: 'sqlcache',
  keyColumns: [ 'companyId', 'orderId', 'orderDetailId' ],
  checksumColumn: 'checksum',
  checksumQuery: `
    SELECT
      companyId, orderId, orderDetailId,
      CHECKSUM(productId, qty) checksum
    FROM
      OrderDetail
  `,
  singleQuery: `
    SELECT
      companyId, orderId, orderDetailId,
      productId, qty,
      CHECKSUM(productId, qty) checksum
    FROM
      OrderDetail
    WHERE
      companyId  = @companyId
      AND orderId = @orderId
      AND orderDetailId = @orderDetailId
  `,
  allQuery: `
    SELECT
      companyId, orderId, orderDetailId,
      productId, qty,
      CHECKSUM(productId, qty) checksum
    FROM
      OrderDetail
  `
});

var refresh = function() {
  orderDetailCache.refresh(
    OrderDetails.find(
      {},
      {
        fields: {
          companyId: 1,
          orderId: 1,
          orderDetailId: 1,
          checksum: 1
        }
      }).fetch()
  );
};

var upsertHandler = Meteor.bindEnvironment(function(row) {
  console.log('upsert:\n', row,
    OrderDetails.upsert({
      companyId: row.companyId,
      orderId: row.orderId,
      orderDetailId: row.orderDetailId
    }, {$set: row})
  );
});

orderDetailCache.on('insert', upsertHandler);
orderDetailCache.on('update', upsertHandler);

orderDetailCache.on('delete', Meteor.bindEnvironment(function(row) {
  console.log('delete:\n', row);
  OrderDetails.remove({
    companyId: row.companyId,
    orderId: row.orderId,
    orderDetailId: row.orderDetailId
  });
}));

module.exports.refresh = refresh;
