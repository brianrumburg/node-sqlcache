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
  keyColumns: [ 'customerId' ],
  checksumColumn: 'checksum',
  checksumQuery: `
    SELECT
      customerId,
      CHECKSUM(name) checksum
    FROM
      Customer
  `,
  // singleQuery: `
  //   SELECT
  //     customerId,
  //     name,
  //     CHECKSUM(name) checksum
  //   FROM
  //     Customer
  //   WHERE
  //     customerId = @customerId
  // `,
  // allQuery: `
  //   SELECT
  //     customerId,
  //     name,
  //     CHECKSUM(name) checksum
  //   FROM
  //     Customer
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
