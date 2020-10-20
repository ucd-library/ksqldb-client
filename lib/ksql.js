const fetch = require('node-fetch');
const consts = require('./consts');

class KsqlClient {

  /**
   * 
   * @param {Object} opts
   * @param {String} opts.url
   * @param {String} opts.apiKey
   * @param {String} opts.secret 
   */
  constructor(opts={}) {
    if( !opts.url ) throw new Error('url required');
    this.opts = opts;
  }

  getHeaders(accept) {
    let headers = {};
    if( this.opts.apiKey && this.opts.secret ) {
      headers.authorization = 'basic '+ base64.encode(this.opts.apiKey + ":" + this.opts.secret);
    }
    if( accept ) {
      headers.accept = accept;
    }
    return headers;
  }

  /**
   * @method query
   * @description execute a ksql api query
   * https://docs.ksqldb.io/en/latest/developer-guide/api/
   * 
   * @param {String} ksql 
   * @param {Object} streamProperties 
   */
  async query(ksql, streamProperties={}) {
    let response = await fetch(
      this.opts.url+consts.PATHS.KSQL,
      {
        method : 'POST',
        headers : this.getHeaders(),
        body : JSON.stringify({
          ksql, streamProperties
        })
      }
    );

    let body = await response.text();
    if( response.status >= 400 ) {
      return new ApiError(response, body);
    }

    return {
      httpResponse : response,
      result : JSON.parse(body)
    }

  }

}

class ApiError {
  constructor(response, body) {
    this.response = response;
    this.rawBody = body;
    this.status = response.status;

    try {
      body = JSON.parse(body);
      this.error_code = body.error_code;
      this.message = body.message;
      this.body = body;
    } catch(e) {
      this.badness = e;
    }
  }
}

module.exports = {ApiError, KsqlClient};