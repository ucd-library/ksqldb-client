# ksqldb-client

Sample Usage:

```js
const {QueryStreamClient} = require("@ucd-lib/ksqldb-client/query-stream");

(async function() {
  let client = new QueryStreamClient({url: 'http://localhost:8088'});

  await client.connect();
  try {
    let results = await client.query('select * from foo_stream EMIT CHANGES;', {limit: 1, returnArray: true});   
    console.log(results);

    results = await client.query(`select * from foo_stream where id = '00000000-0000-0000-0000-000000000000' EMIT CHANGES;`, {limit: 1, returnArray: true});
    console.log(results);
  } catch(e) {
    console.log(e);
  }
  await client.disconnect();
  console.log('done');
})();

```

## QueryStreamClient.client(query, opts) Arguments
```js
  /**
   * @param {String} query sql query 
   * @param {Object} opts query options
   * @param {Function} opts.handler callback function for this query
   * @param {Number} opts.timeout set idle window to close query
   * @param {Number} opts.limit limit result set
   * @param {Boolean} opts.first set limit to 1
   * @param {Boolean} opts.returnArray return result set as array
   */
```