const pg = require('pg')

module.exports = {
  getClient: () => {
    const client = new pg.Client(process.env.PG_URL)
    client.connect()
    return client
  },
}
