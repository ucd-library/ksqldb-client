module.exports = {
  HEADERS : {
    ACCEPT : {
      JSON : 'application/json',
      KSQL_JSON : 'application/vnd.ksql.v1+json',
      KSQL_DELIMINTED : 'application/vnd.ksqlapi.delimited.v1'
    }
  },
  PATHS : {
    QUERY : '/query',
    QUERY_STREAM : '/query-stream',
    KSQL : '/ksql'
  }
}