const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const insert = require('./insert');

const mongoUrl = process.env.MONGO_URL;
const totalQuery = process.env.TOTAL_QUERY;
const writeConcern = parseInt(process.env.WRITE_CONCERN);
const hostIndex = parseInt(process.env.HOST_INDEX);

if (!mongoUrl || !totalQuery || writeConcern === undefined || hostIndex === undefined) {
  console.log('usage: MONGO_URL="mongodb://~" TOTAL_QUERY=10000 WRITE_CONCERN=1 HOST_INDEX=0 node index.js');
  process.exit(1);
}

const promises = [];

if (cluster.isMaster) {
  const numOfQuery = Math.ceil(totalQuery / numCPUs);
  for (let i = 0; i < numCPUs; i++) {
    const offset = hostIndex * totalQuery + numOfQuery * i;

    const worker = cluster.fork();
    const promise = new Promise((resolve, reject) => {
      worker.on('exit', (code, signal) => {
        if (signal) {
          reject({ signal });
        } else if (code !== 0) {
          reject({ code });
        } else {
          resolve();
        }
      });
    });
    worker.send({
      mongoUrl,
      offset,
      numOfQuery,
    });
    promises.push(promise);
  }
} else {
  process.on('message', function(msg) {
    insert(msg.mongoUrl, msg.offset, msg.numOfQuery, writeConcern, hostIndex)
      .then(() => {
        process.exit(0);
      })
      .catch((e) => {
        console.error(e);
        process.exit(1);
      });
  });
}

Promise.all(promises)
  .then(() => {
  })
  .catch((e) => {
    console.error(e);
  });
