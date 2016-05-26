var cache = require('../../');
const rl = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});

cache.init({
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
  // singleQuery: `
  //   SELECT
  //     companyId, orderId, orderDetailId,
  //     productId, qty,
  //     CHECKSUM(productId, qty) checksum
  //   FROM
  //     OrderDetail
  //   WHERE
  //     companyId  = @companyId
  //     AND orderId = @orderId
  //     AND orderDetailId = @orderDetailId
  // `,
  // allQuery: `
  //   SELECT
  //     companyId, orderId, orderDetailId,
  //     productId, qty,
  //     CHECKSUM(productId, qty) checksum
  //   FROM
  //     OrderDetail
  // `
});

var readLine = function() {
  setTimeout(function(){
    rl.question('[enter] to refresh, [q] to exit: ', (answer) => {
      if(answer == '') {
        cache.refresh();
        readLine();
      } else {
        cache.close();
        process.exit();
      }
    });
  }, 500);
};

readLine();
