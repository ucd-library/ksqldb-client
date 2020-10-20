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

  connect() {
    this.session = http2.connect(
      this.opts.url+consts.PATHS.QUERY_STREAM
    );
  }

  disconnect() {
    return new Promise((resolve, reject) => {
      this.session.close(resolve);
    });
  }

  async query(query, opts={}) {
    this.queryHandler = opts.handler;
    this.timeout = opts.timeout;

    return new Promise((resolve, reject) => {
      let stream = this._initQuery(query, opts.properties);

      this.stream = stream;
      this.resetTimeout();

      stream.on('response', headers => {
        const status = headers[HTTP2_HEADER_STATUS];
        if (Number(status) > 302 || isNaN(status)) {
          return reject(new Error(`query failed. Status ${headers[HTTP2_HEADER_STATUS]}`));
        }

        stream
          .on('error', error => resolve(reject(error)))
          .on('aborted', () => resolve(reject(new Error('abort'))))
          .on('timeout', () => resolve(reject(new Error('timeout'))))
          .on('close', () => resolve());

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
    this.timerId = null;
    this.stream.destroy();
  }

  isCompleteJson(json) {
    return (json[json.length-1] === ']' || json[json.length-1] === '}');
  }

  // TODO: switch to https://www.npmjs.com/package/stream-json ?
  doWork() {
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
        if( this.opts.handler ) this.opts.handler(json);
        if( this.queryHandler ) this.queryHandler(json);
        rawJson = '';
      } catch(e) {
        // didn't complete json
      }
    }

    // return unfinished json
    if( rawJson !== '' ) {
      this.received.unshift(rawJson);
    }

    const next = this.stream.read();
    if (next != null) {
      const nextLines = this.parseChunk(next);
      this.received.push(...nextLines);
      this.resetTimeout();
    }

    setImmediate(() => this.doWork());
  };

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
    const stream = this.session.request(this.headers());
    const reqPayload = Buffer.from(JSON.stringify(query));
    
    stream.end(reqPayload);
    return stream;
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