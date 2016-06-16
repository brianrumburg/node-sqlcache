import { Meteor } from 'meteor/meteor';
import sc from 'sqlcache';
import { ProductTypes } from '../../collections'

var cache = sc.create({
  connectionString: Meteor.settings.connectionString,
  verbose: Meteor.settings.verbose,
  keyColumns: [ 'productTypeId' ],
  checksumColumn: 'checksum',
  checksumQuery: `
  SELECT
    productTypeId,
    CHECKSUM(productTypeDescription) checksum
  FROM
    [ProductType]
`,
  singleQuery: `
  SELECT
    productTypeId, productTypeDescription,
    CHECKSUM(productTypeDescription) checksum
  FROM
    [ProductType]
  WHERE
    productTypeId = @productTypeId
`
});

var refresh = function() {
  return cache.refresh(ProductTypes.find({},{
    fields: {
      productTypeId: 1,
      checksum: 1
    }
  }).fetch());
};

cache.on('ready', Meteor.bindEnvironment(() => {
  console.log('ProductType cache ready. Refreshing...');
  refresh().then(() => {
    console.log('ProductType cache refresh complete.');
  }).catch((err) => {
    console.log('ProductType cache init error:', err.stack);
  });
}));

var upsertHandler = Meteor.bindEnvironment(function(row) {
  ProductTypes.upsert({
    productTypeId: row.productTypeId
  }, {$set: row});
});

cache.on('insert', upsertHandler);
cache.on('update', upsertHandler);

cache.on('delete', Meteor.bindEnvironment(function(row) {
  ProductTypes.remove({
    productTypeId: row.productTypeId
  });
}));

module.exports.refresh = refresh;
