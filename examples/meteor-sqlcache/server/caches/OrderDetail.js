import { Meteor } from 'meteor/meteor';
import sc from 'sqlcache';

var cache = sc.create({
  connectionString: Meteor.settings.connectionString,
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
      companyId = @companyId
      AND orderId = @orderId
      AND orderDetailId = @orderDetailId
  `
});

var refresh = function() {
  cache.refresh(
    OrderDetails.find({},{
      companyId: 1,
      orderId: 1,
      orderDetailId: 1,
      checksum: 1
    }).fetch()
  );
};

var upsertHandler = Meteor.bindEnvironment(function(row) {
  OrderDetails.upsert({
    companyId: row.companyId,
    orderId: row.orderId,
    orderDetailId: row.orderDetailId
  }, {$set: row});
});

cache.on('insert', upsertHandler);
cache.on('update', upsertHandler);

cache.on('delete', Meteor.bindEnvironment(function(row) {
  OrderDetails.remove({
    companyId: row.companyId,
    orderId: row.orderId,
    orderDetailId: row.orderDetailId
  });
}));

module.exports.refresh = refresh;
