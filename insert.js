const MongoClient = require('mongodb').MongoClient;

const pid = process.pid;

function _insert(mongoUrl, offset, numOfQuery, writeConcern) {
  return new Promise((resolve, reject) => {
    MongoClient.connect(mongoUrl, function(err, db) {
      if (err) {
        console.error(err);
        process.exit(0);
      }
      const promises = [];

      for (let i = 0; i < numOfQuery; i++) {
        const promise = db.collection('test').insert({
          '_id': offset + i + 1,
          'balance': 10000,
          'txs': [],
          'processId': pid,
          'createdAt': new Date(),
          'updatedAt': new Date(),
        }, {
          w: writeConcern,
        });

        promises.push(promise);
      }

      Promise.all(promises)
        .then(() => {
          db.close();
          resolve();
        })
      .catch((e) => {
        console.error(e);
        db.close();
        reject();
      });
    });
  });
}

const QUERY_PER_CONNECTION = 1000;

function run(mongoUrl, offset, numOfQuery, writeConcern) {
  const n = Math.floor(numOfQuery / QUERY_PER_CONNECTION);
  const promises = [];

  for (let i = 0; i < n; i++) {
    const promise = _insert(mongoUrl, offset + QUERY_PER_CONNECTION * i, QUERY_PER_CONNECTION, writeConcern);
    promises.push(promise);
  }

  const promise = _insert(mongoUrl, offset + QUERY_PER_CONNECTION * n, numOfQuery - QUERY_PER_CONNECTION * n, writeConcern);
  promises.push(promise);

  return Promise.all(promises);
}

module.exports = run;
