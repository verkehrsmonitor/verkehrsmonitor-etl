const fs = require('fs');
const path = require('path');
const unzip = require('unzip');
const axios = require('axios');

const config = require('../config');
const utils = require('../lib/utils');

// datasets are divided into Autobahn and Bundesstraße
const categories = config.categories || ['A', 'B'];

// data is available from the year 2003 to the year 2016
const startYear = config.fromYear || 2003;
const endYear = config.toYear || 2016;

async function processData() {
    const downloadQueue = [];
    for (const category of categories) {
        for (const year of utils.range(startYear, endYear)) {
            const hourData = config.format.hour(year, category);
            for (exception of config.exceptions) {
                if (exception.wrong(year, category)) {
                    Object.assign(hourData, exception.correct(year, category));
                }
            }
            downloadQueue.push(hourData);

            const yearData = config.format.year(year, category);
            downloadQueue.push(yearData);
        }
    }

    await runLoop(downloadQueue);
};

async function runLoop(downloadQueue) {
    for (const [index, target] of await downloadQueue.entries()) {
        try {
            await download(target);
        } catch (err) {
            console.error('Error processing the file:', target.url);
        }
    }
}

async function download(target) {
    return new Promise(async (resolve, reject) => {
        const { url, out, name } = target;
        const request = {
            url,
            responseType: 'stream'
        };

        try {
            const response = await axios(request);

            let output;
            if (url.endsWith('.zip')) {
                output = unzip.Extract({path: out});
            } else {
                const file = path.resolve(out, name);
                output = fs.createWriteStream(file);
            }

            const stream = response.data
                .pipe(output)
                .on('finish', resolve)
                .on('error', reject);
        } catch (err) {
            reject(err);
        }
    });
};

module.exports = processData();
