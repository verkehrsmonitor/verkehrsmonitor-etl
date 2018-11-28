const moment = require('moment');

module.exports = {
    db: {
        host: 'localhost',
        port: 27017,
        name: 'test'
    },
    categories: ['A', 'B'],
    fromYear: 2003,
    toYear: 2004,
    networkList: './data/network_list.tsv',
    format: {
        year: (year, category) => {
            return {
                url: `http://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung/Daten/${year}_1/Jawe${year}.csv?view=renderTcDataExportCSV&cms_strTyp=${category}`,
                name: `geo_${year}_${category}.csv`,
                out: 'data',
                collection: 'stations',
                transform: data => {
                    return {
                        nr: Number(data.DZ_Nr),
                        name: data.DZ_Name,
                        land: data.Land_Code,
                        roadid: Number(data.Str_Nr),
                        type: data.Str_Kl,
                        lat: Number(data.Koor_WGS84_N.replace(',', '.')),
                        lng: Number(data.Koor_WGS84_E.replace(',', '.')),
                        letter: data.Str_Zus
                    };
                }
            }
        },
        hour: (year, category) => {
            const num = n => n === -1 ? null : Number(n); // turn -1 to null

            return {
                url: `http://www.bast.de/videos/${year}_${category}_S.zip`,
                name: `${year}_${category}_S.txt`,
                out: 'data',
                collection: 'measures',
                transform: data => {
                    return {
                        nr: num(data.Zst),
                        date: moment(data.Datum, "YYMMDD").hours(data.Stunde).toDate(),
                        total_1: num(data.KFZ_R1),
                        total_2: num(data.KFZ_R2),
                        car_1: num(data.PLZ_R1),
                        car_2: num(data.PLZ_R2),
                        truck_1: num(data.Lkw_R1) - num(data.Bus_R1),
                        truck_2: num(data.Lkw_R2) - num(data.Bus_R2),
                        bus_1: num(data.Bus_R1),
                        bus_2: num(data.Bus_R2),
                        type: data.Strklas
                    };
                }
            }
        }
    },
    exceptions: [
        // there are some file that don't use the naming convention
        {
            wrong: (year, category) => {
                return year === 2015 && category === 'A';
            },
            correct: (year, category) => {
                return {
                    url: `http://www.bast.de/videos/${year}_B${category}B_S.zip`,
                    name: `${year}_B${category}B_S.txt`
                };
            }
        },
        {
            wrong: (year, category) => {
                return year === 2014
            },
            correct: (year, category) => {
                return {
                    name: `DZ_${year}_${category}_S.txt`
                };
            }
        }
    ],
    highwayExceptions: [
        {
            name: 'Bundesautobahn 2',
            osmid: '3140168'
        },
        {
            name: 'Bundesautobahn 3',
            osmid: '2925465'
        },
        {
            name: 'Bundesautobahn 9',
            osmid: '2925468'
        }
    ],
    highwayOut: 'data'
}
