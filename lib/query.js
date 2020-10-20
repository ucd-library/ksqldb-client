const WebSocketClient = require('websocket').client;
const consts = require('./consts')

class QueryClient {

  constructor(opts) {
    this.opts = opts;
    this.client = new WebSocketClient();
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.client.on('connectFailed', e => reject(e));
      this.client.on('connect', connection => {
        connection.on('error', e => console.error('Connection Error', e));
        // connection.on('close', function() {
        //     console.log('echo-protocol Connection Closed');
        // });
        connection.on('message', message => {
          if( message.type === 'utf8' ) {
            this._queryHandler(message.utf8Data);
          } else {
            console.warn('Unsupported message type', message);
          }
        });
        this.connection = connection;
        resolve();
      });

      console.log('ws://'+this.opts.host+consts.PATHS.QUERY);
      this.client.connect('ws://'+this.opts.host+consts.PATHS.QUERY, 'echo-protocol');
    });
  }

  query(ksql) {
    this.connection.sendUTF(ksql);
  }

  _queryHandler(data) {
    console.log(data);
  }
}

module.exports = {QueryClient};