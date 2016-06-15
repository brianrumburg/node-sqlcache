import { Meteor } from 'meteor/meteor';
import sc from 'sqlcache';
import { OrderDetails } from '../../collections'

var cache = sc.create({
  connectionString: Meteor.settings.connectionString,
  verbose: true,
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

var refresh = () => {
  return cache.refresh(OrderDetails.find({},{
      companyId: 1,
      orderId: 1,
      orderDetailId: 1,
      checksum: 1
    }).fetch()
  );
};

cache.on('ready', Meteor.bindEnvironment(() => {
  console.log('OrderDetail cache ready. Refreshing...');
  refresh().then(() => {
    console.log('OrderDetail cache refresh complete.');
  }).catch((err) => {
    console.log('OrderDetail cache init error:', err.stack);
  });
}));

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
