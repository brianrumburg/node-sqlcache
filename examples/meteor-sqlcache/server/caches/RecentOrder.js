import { Meteor } from 'meteor/meteor';
import sc from 'sqlcache';
import { RecentOrders } from '../../collections'
import moment from 'moment';

var cache = sc.create({
  connectionString: Meteor.settings.connectionString,
  verbose: Meteor.settings.verbose,
  keyColumns: [ 'companyId', 'orderId' ],
  checksumColumn: 'checksum',
  singleQuery: `
  SELECT
    companyId, orderId, customerId, orderDate,
    CHECKSUM(customerId, orderDate) checksum
  FROM
    [Order]
  WHERE
    companyId = @companyId
    AND orderId = @orderId
`
});

var refresh = function() {
  var checksumQuery = `
    SELECT
      companyId, orderId,
      CHECKSUM(customerId, orderDate) checksum
    FROM
      [Order]
    WHERE
      orderDate > '${moment().subtract(1, 'minutes').toISOString()}'
  `;

  return cache.refresh(RecentOrders.find({},{
    fields: {
      companyId: 1,
      orderId: 1,
      checksum: 1
    }
  }).fetch(), checksumQuery);
};

cache.on('ready', Meteor.bindEnvironment(() => {
  Meteor.setInterval(() => {
    console.log('RecentOrder cache refreshing...');
    refresh().then(() => {
      console.log('RecentOrder cache refresh complete.');
    }).catch((err) => {
      console.log('RecentOrder cache refresh error:', err.stack);
    });
  }, 10000);
}));

var upsertHandler = Meteor.bindEnvironment(function(row) {
  RecentOrders.upsert({
    companyId: row.companyId,
    orderId: row.orderId
  }, {$set: row});
});

cache.on('insert', upsertHandler);
cache.on('update', upsertHandler);

cache.on('delete', Meteor.bindEnvironment(function(row) {
  RecentOrders.remove({
    companyId: row.companyId,
    orderId: row.orderId
  });
}));

module.exports.refresh = refresh;
