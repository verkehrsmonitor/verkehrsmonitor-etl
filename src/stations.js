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

async function processData() {
    const fileQueue = [];

    let collection = '';
    // for each category
    for (const category of categories) {
        // for each year
        for (const year of utils.range(startYear, endYear)) {
            const stations = config.format.year(year, category);
            collection = stations.collection;

            // process data
            fileQueue.push({stations, year});
        }
    }

    const allStations = {};
    const results = {};
    for (const target of fileQueue) {
        const stations = processFile(target, allStations);
        stations.forEach(station => {
            results[station.nr] = station;
        })
    }

    // create the DB connection
    const dbPort = config.db.port || 27017;
    const dbHost = config.db.host || 'localhost';
    const dbName = config.db.name || 'test';
    const dbURL = `mongodb://${dbHost}:${dbPort}/`;
    const dbConfig = { dbURL, collection, dbName };
    const db = new StreamToMongoDB(dbConfig).stream;

    Object.values(results).forEach(r => {
        db.write(r);
    });
};

function processFile(target, allStations) {
    const { name, out, transform } = target.stations;
    const { year } = target;

    // read files
    const networkFileName = path.resolve(config.networkList);
    const networkFile = fs.readFileSync(networkFileName, { encoding: 'latin1' });
    const stationFileName = path.resolve(out, name);
    const stationFile = fs.readFileSync(stationFileName, { encoding: 'latin1'});

    // parse from CSV
    const stations = csvSync(stationFile, {columns: true, delimiter: ';', trim: true});
    const network = csvSync(networkFile, {columns: ['nr', 'type', 'roadid', 'letter'] , delimiter: '\t', trim: true, auto_parse: true});

    const transformed = stations.map(transform);
    return transformed.map(s => {
        const road = network.filter(n => (n.roadid === s.roadid) && (n.letter === s.letter) && (n.type === s.type));
        const nodeIndex = road.findIndex(n => (n.nr === s.nr));
        s.prev = null;
        s.next = null;

        const prev = road[nodeIndex - 1];
        if (prev) {
            const prevStation = transformed.find(station => station.nr === prev.nr) || allStations[prev.nr];
            s.prev = prevStation && prev.nr;
        }

        const next = road[nodeIndex + 1];
        if (next) {
            const nextStation = transformed.find(station => station.nr === next.nr) || allStations[next.nr];
            s.next = nextStation && next.nr;
        }

        allStations[s.nr] = allStations[s.nr] ? allStations[s.nr].add(year) : new Set([year]);
        s.years = [...allStations[s.nr]];
        return s;
    });
};

module.exports = processData();
