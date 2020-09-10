// Working fork of https://github.com/AbdullahAli/node-stream-to-mongo-db

const { Writable } = require('stream');
const { MongoClient } = require('mongodb');

class StreamToMongoDB {

  constructor(options) {
    this.config = {};
    this.batch = [];
    this.dbConnection = null;
    this.defaultConfig = {
      batchSize: 1,
      insertOptions: { ordered: false, w: 1 }
    };

    this.setupConfig(options);
    this.stream = this.writableStream();
  }

  stream() {
    return this.stream;
  }

  async connect() {
    const conn = await MongoClient.connect(this.config.dbURL);
    return conn;
  };

  async insertToMongo(records) {
    records = records.map(r => {
      delete r._id;
      return r;
    });

    try {
      await this.dbConnection.db(this.config.dbName).collection(this.config.collection).insert(records, this.config.insertOptions);
      this.resetBatch();
    } catch (error) {
      console.log(error);
    }
  };

  async addToBatch(record) {
    try {
      this.batch.push(record);

      if (this.batch.length === this.config.batchSize) {
        await this.insertToMongo(this.batch);
      }
    } catch (error) {
      console.log(error);
    }
  }

  setupConfig(options) {
    // add required options if not exists
    Object.keys(this.defaultConfig).forEach((configKey) => {
      if(!options[configKey]) {
        options[configKey] = this.defaultConfig[configKey];
      }
    });

    this.config = options;
  };

  resetConn() {
    if (this.dbConnection)
      this.dbConnection.close();
    this.dbConnection = null;
  };

  resetBatch() {
    this.batch = [];
  };

  writableStream() {
    const writable = new Writable({
      objectMode: true,
      write: async (record, encoding, next) => {
        try {
          if (this.dbConnection) {
            await this.addToBatch(record);
            next();
          } else {
            this.dbConnection = await this.connect();
            await this.addToBatch(record);
            next();
          }
        } catch (error) {
          console.log(error);
        }
      }
    });

    writable.on('finish', async () => {
      try {
        if(this.batch.length) {
          await this.insertToMongo(batch);
        }
        this.dbConnection.close();
        this.resetConn();
        writable.emit('close');
      } catch(error) {
        console.log(error);
      }
    });

    return writable;
  };

};

module.exports = { StreamToMongoDB };
