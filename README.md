# BASt - Traffic Count ETL Process

This script will download, parse and import data from [bast.de](http://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/Stundenwerte.html) to a mongodb instance.

## Prerequisites:

- MongoDB >= 3.7
- Node.js >= 9

## Installation

```
npm install
```

## Config

You can configure the importer in the `config.js` file:

- `db`:
  - `host`: the hostname where your MongoDB instance is running
  - `port`: the port to communicate with your MongoDB instance
  - `name`: the name of the database
- `categories`: an array containing `'A'` for Autobahn and `'B'` for Bundesstraße
- `fromYear`: starting year for importing data (min. `2003`)
- `toYear`: end year of the imported data (max. `2016`)
- `networkList`: a sorted list of measurement stations
- `format`:
  - `year`: a function called for each file containing measurement stations
  - `hour`: a function called for each file hourly collected data
- `exceptions`: an array of objects containing exceptions used to correct problems in the data. Each object contains a `wrong` function to identify the file containing the error and a `correct` function to correct the problems
- `highwayExceptions`: an array of objects containing the name of an Autobahn and its ID in OpenStreetMap. This is needed since some entries can't be found by name
- `highwayOut`: the folder where you want to save the GeoJSON files extracted from OpenStreetMap

## Run:

- `download` will download the data
```
npm run download
```

- `import` will import the hourly measurements to the database
```
npm run import
```

- `stations` will import the data about the measurement stations
```
npm run stations
```

- `wikidata` will query the SPARQL endpoint of [wikidata](https://query.wikidata.org/) in order to obtain additional information about each Autobahn and Bundesstraße. This script will also try to find the OpenStreetMap ID of each entry in order to download a GeoJSON file of the Autobahn/Bundesstraße
```
npm run wikidata
```

The script `scripts/aggregate.js` is a stub used to aggregate and reshape the imported data using the MongoDB query language.
