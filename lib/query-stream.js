const http2 = require('http2');
const consts = require('./consts')

const {
  HTTP2_HEADER_PATH,
  HTTP2_HEADER_STATUS,
  HTTP2_HEADER_CONTENT_TYPE,
  HTTP2_HEADER_METHOD,
} = http2.constants;

class QueryStreamClient {

  constructor(opts) {
    this.opts = opts;
    this.received = [];
  }

  /**
   * @description open client connection to server
   */
  connect() {
    this.client = http2.connect(
      this.opts.url+consts.PATHS.QUERY_STREAM
    );
  }

  disconnect() {
    return new Promise((resolve, reject) => {
      this.client.close(() => resolve());
    });
  }

  /**
   * 
   * @param {String} query sql query 
   * @param {Object} opts query options
   * @param {Function} opts.handler callback function for this query
   * @param {Number} opts.timeout set idle window to close query
   * @param {Number} opts.limit limit result set
   * @param {Boolean} opts.first set limit to 1
   * @param {Boolean} opts.returnArray return result set as array
   */
  async query(query, opts={}) {
    if( opts.first ) opts.limit = 1;
    this.queryHandler = opts.handler;
    this.timeout = opts.timeout;
    this.maxResults = opts.limit || null;
    this.returnArray = opts.returnArray || false;

    this.resultCount = 0;
    this.results = [];
    this.schema = [];

    return new Promise((resolve, reject) => {
      this.req = this._initQuery(query, opts.properties);

      this.resetTimeout();

      this.req.on('response', async headers => {
        const status = headers[HTTP2_HEADER_STATUS];
        if (Number(status) > 302 || isNaN(status)) {
          let body = await this._readError();
          try {
            reject(JSON.parse(body));
          } catch(e) {
            reject(body);
          }
        }

        this.req
          .on('error', error => resolve(reject(error)))
          .on('aborted', () => resolve(reject(new Error('abort'))))
          .on('timeout', () => resolve(reject(new Error('timeout'))))
          .on('close', () => {
            if( this.returnArray ) resolve(this.results);
            else resolve();
          });

        this.doWork();
      });
    });
  }

  resetTimeout() {
    if( !this.timeout || this.timeout === -1 ) return;
    if( this.timerId ) clearTimeout(this.timerId);
    this.timerId = setTimeout(() => this.close(), this.timeout);
  }

  close() {
    if( this.timerId ) clearTimeout(this.timerId);
    this.timerId = null;
    this.req.destroy();
    this.req = null;
  }

  isCompleteJson(json) {
    return (json[json.length-1] === ']' || json[json.length-1] === '}');
  }

  // TODO: switch to https://www.npmjs.com/package/stream-json ?
  doWork() {
    if( !this.req ) return;

    let rawJson = '';
    while( this.received.length ) {
      rawJson += this.received.shift();

      while( !this.isCompleteJson(rawJson) && this.received.length ) {
        rawJson += this.received.shift();
      }

      if( !this.isCompleteJson(rawJson) ) {
        continue;
      }

      try {
        let json = JSON.parse(rawJson);
        this.resultCount++;

        if( this.resultCount === 1 ) {
          this.schema = json.columnNames;
          rawJson = '';
          continue;
        }

        let result = {};
        json.forEach((val, i) => result[this.schema[i]] = val);

        if( this.opts.handler ) this.opts.handler(result);
        if( this.queryHandler ) this.queryHandler(result);
        if( this.returnArray ) this.results.push(result);

        // first result is definition
        if( this.maxResults !== null && this.resultCount >= this.maxResults+1 ) {
          this.close();
          return;
        }
        rawJson = '';
      } catch(e) {
        // didn't complete json
      }
    }

    // return unfinished json
    if( rawJson !== '' ) {
      this.received.unshift(rawJson);
    }

    const next = this.req.read();
    if (next != null) {
      const nextLines = this.parseChunk(next);
      this.received.push(...nextLines);
      this.resetTimeout();
    }

    setImmediate(() => this.doWork());
  };

  _readError() {
    let body = '';
    return new Promise((resolve, reject) => {
      this.req
        .on('data', (chunk) => {
          body += chunk.toString();
        })
        .on('end', () => resolve(body));
    });
  }

  parseChunk(buf) {
    return buf
      .toString()
      .split('\n')
      .filter(str => str);
  }

  _initQuery(sql, properties) {
    if( !properties ) {
      properties = {
        'auto.offset.reset': 'earliest'
      }
    }

    let query = {sql, properties};
    const client = this.client.request(this.headers());
    const reqPayload = Buffer.from(JSON.stringify(query));
    
    client.end(reqPayload);
    return client;
  }

  headers() {
    return {
      [HTTP2_HEADER_PATH]: '/query-stream',
      [HTTP2_HEADER_METHOD]: 'POST',
      [HTTP2_HEADER_CONTENT_TYPE]: consts.HEADERS.ACCEPT.KSQL_DELIMINTED
    };
  }

}

module.exports = {QueryStreamClient};