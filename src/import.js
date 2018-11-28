const fs = require('fs');
const path = require('path');
const csv = require('csv-parse');
const csvSync = require('csv-parse/lib/sync');
const transformer = require('stream-transform');
const { StreamToMongoDB } = require('../lib/mongostream');

const config = require('../config');
const utils = require('../lib/utils');

// datasets are divided into Autobahn and Bundesstraße
const categories = config.categories || ['A', 'B'];

// data is available from the year 2003 to the year 2016
const startYear = config.fromYear || 2003;
const endYear = config.toYear || 2016;

async function processFiles() {

    // for each category
    for (const category of categories) {
        // for each year
        for (const year of utils.range(startYear, endYear)) {

            // compile URL and name using templating
            const data = config.format.hour(year, category);
            const stations = config.format.year(year, category);

            // correct exceptions (non-standard filenames, URLs...)
            for (const exception of config.exceptions) {
                if (exception.wrong(year, category)) {
                    Object.assign(data, exception.correct(year, category));
                }
            }

            // process data
            processFile({data, stations});
        }
    }

};

function processFile(target) {
    return new Promise((resolve, reject) => {
        const { name, out, transform, collection } = target.data;

        // read files
        const file = path.resolve(out, name);
        const stream = fs.createReadStream(file, { encoding: 'latin1' });
        const stationFileName = path.resolve(out, target.stations.name);
        const stationFile = fs.readFileSync(stationFileName, { encoding: 'latin1'});

        // parse from CSV
        const stations = csvSync(stationFile, {columns: true, delimiter: ';', trim: true});
        const csvStream = csv({
            columns: true,
            delimiter: ';',
            trim: true
        });

        // format and merge data
        const format = transformer(transform);
        const merge = transformer(data => {
            const found = stations.find(s => parseInt(s.DZ_Nr) === data.nr);
            return Object.assign(data, target.stations.transform(found));
        });

        // create the DB connection
        const dbPort = config.db.port || 27017;
        const dbHost = config.db.host || 'localhost';
        const dbName = config.db.name || 'test';
        const dbURL = `mongodb://${dbHost}:${dbPort}/`;
        const dbConfig = { dbURL, collection, dbName };
        const db = new StreamToMongoDB(dbConfig).stream;

        // run the pipeline
        stream
            .pipe(csvStream)
            .pipe(format)
            .pipe(merge)
            .pipe(db)
            .on('end', resolve)
            .on('error', reject);
    });
};

module.exports = processFiles();
