const fs = require('fs');
const path = require('path');
const axios = require('axios');
const osmtogeojson = require('osmtogeojson');
const { DOMParser } = require('xmldom');
const geojsonMerge = require('@mapbox/geojson-merge');
const mapshaper = require('mapshaper');

const config = require('../config');

async function wikiData({
        endpoint = 'https://query.wikidata.org/sparql',
        format = 'json',
        type = 'A',
        offset = 0,
        limit = 300
    } = {}) {

    let queryType = '';
    switch (type) {
        case 'A':
            queryType = `VALUES (?autobahn) {
                    (wd:Q313301)
                }
                ?highway wdt:P16 ?autobahn.`;
            break;
        case 'B':
            queryType = `VALUES (?autobahn) {
                    (wd:Q561431)
                }
                ?highway wdt:P31 ?autobahn.`;
            break;
    };

    const sparqlQuery =
    `SELECT ?highwayLabel ?length (GROUP_CONCAT(DISTINCT ?terminusL; SEPARATOR = ",") AS ?termini) (GROUP_CONCAT(?connectedL; SEPARATOR = ",") AS ?cities) (GROUP_CONCAT(DISTINCT ?coordinate_location; SEPARATOR = ",") AS ?coords) (GROUP_CONCAT(DISTINCT ?osm; SEPARATOR = ",") AS ?osmid) WHERE {
      ${queryType}
      ?highway wdt:P609 ?terminus.
      OPTIONAL {
        ?terminus rdfs:label ?terminusL.
        FILTER((LANG(?terminusL)) = "de")
      }
      OPTIONAL { ?terminus wdt:P625 ?coordinate_location. }
      OPTIONAL { ?highway wdt:P2043 ?length. }
      OPTIONAL {
        ?highway wdt:P2789 ?connected.
        ?connected rdfs:label ?connectedL.
        FILTER((LANG(?connectedL)) = "de")
      }
      OPTIONAL {
        ?highway wdt:P402 ?osm
      }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "de". }
    }
    GROUP BY ?highway ?highwayLabel ?length ?osmid
    ORDER BY ?highwayLabel
    LIMIT ${limit}
    OFFSET ${offset}`;

    const url = endpoint + '?query=' + encodeURIComponent(sparqlQuery);
    let headers = {};
    switch (format) {
        case 'json':
            headers = { 'Accept': 'application/json'};
            break;

        case 'csv':
            headers = { 'Accept': 'text/csv'};
            break;
    };

    try {
        const response = await axios({ url, headers });
        if (format === 'json'){
            return response.data.results.bindings;
        }
        return response.data;
    } catch(err) {
        console.error(`Error requesting ${endpoint}:\n`, err);
    }

};

async function requestOSM(osmid) {
    if (osmid.length > 0) {
        const url = `https://api.openstreetmap.org/api/0.6/relation/${osmid}/full`
        try {
            // request OSM data
            const response = await axios({ url });
            // convert to GeoJSON
            const doc = new DOMParser().parseFromString(response.data);
            const geojson = osmtogeojson(doc);
            // clean the GeoJSON
            geojson.features = geojson.features.filter(feature => feature.geometry.type !== 'Point');


            // delete unwanted properties
            const allowed = ['name', 'reg_name', 'lanes', 'maxspeed', 'ref'];
            geojson.features.forEach((feature, i) => {
                Object.keys(feature.properties).forEach(prop => {
                    if (!allowed.includes(prop)) {
                        delete geojson.features[i].properties[prop];
                    }
                })
            });
            return geojson;
        } catch (err) {
            console.error(err);
        }
    }
}

function merge(geojsons, cb) {
    // merge the GeoJSON files
    const merged = geojsonMerge.merge(geojsons);
    // merge the GeoJSON features to GeometryCollection
    const cmd = '-i input.geojson -dissolve -o output.geojson';
    mapshaper.applyCommands(cmd, {'input.geojson': merged}, (err, output) => {
        const geojson = JSON.parse(output['output.geojson']);
        cb(err, geojson);
    });
}

async function osm(data, exceptions, output) {
    for (const highway of data) {
        // format wikidata response
        const arr = ['osmid', 'termini', 'cities'];
        Object.keys(highway).forEach(key => highway[key] = highway[key].value);
        arr.forEach(a => highway[a] = highway[a].split(','));

        // Correct linked OSM IDs
        for (const exception of exceptions) {
            if (highway.highwayLabel === exception.name) {
                highway.osmid = [exception.osmid];
            }
        }

        const geojsons = [];
        for (const osmid of highway.osmid) {
            const geojson = await requestOSM(osmid);
            geojsons.push(geojson);
        }

        // merge properties to the top level
        const formatted = geojsons.map(geojson => {
            if (!geojson) return;
            if (!geojson.features.length > 0) return;
            const props = {};
            geojson.properties = geojson.features.forEach(feature => Object.keys(feature.properties).forEach(k => props[k] ? props[k].add(feature.properties[k]) : props[k] = new Set([feature.properties[k]])));
            geojson.properties = Object.keys(props).reduce((prev, prop) => {
                prev[prop] = [...props[prop]];
                return prev;
            }, {});
            return geojson;
        });

        if (formatted[0]) {
            merge(formatted, (err, merged) => {
                // @TOOD: fix to merge all properties of formatted
                merged.properties = formatted[0].properties;
                merged.properties.name = highway.highwayLabel.replace('Bundesautobahn ', 'A');
                merged.properties.length = highway.length;
                merged.properties.termini = highway.termini;

                const outputPath = path.resolve(output, `${merged.properties.name}.geojson`);
                fs.writeFile(outputPath, JSON.stringify(merged), err => console.error);
            });
        } else {
            console.log(`Data for ${highway.highwayLabel} is missing`);
        }

    }
}

async function main() {
    const exceptions = config.highwayExceptions || [];
    const output = config.highwayOut || '';

    // Get data from wikidata
    const data = await wikiData();
    await osm(data, exceptions, output);
}

main();
