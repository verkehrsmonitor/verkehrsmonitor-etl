async function aggregate() {
    const mongoClient = new MongoClient(dbURL);
    const client = await mongoClient.connect();
    const db = client.db(dbName);
    const collection = db.collection('measures');

    await collection.createIndex({nr: 1});
    await collection.createIndex({type: 1, roadid: 1, date: 1});
    await collection.aggregate(
        [
            {
                $lookup: {
                    from: 'stations',
                    localField: 'nr',
                    foreignField: 'nr',
                    as: 'station'
                }
            },
            {
                $group: {
                    _id: {
                        nr: '$nr',
                        type: '$type',
                        station: '$station'
                    },
                    year: { $addToSet: {$year: '$date'} }
                }
            },
            {
                $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$_id.station", 0 ] }, "$$ROOT" ] } }
            },
            {
                $project: {
                    _id: false
                }
            },
            {
                $out: 'locations'
            }
        ]
    );

    await collection.aggregate([{
        $group: {
          _id: {
            year: { $year: "$date" },
            month: { $month: "$date" },
            day: { $dayOfMonth: "$date" },
            name: '$name',
            land: '$land',
            type: '$type',
            roadid: '$roadid',
            lat: '$lat',
            lng: '$lng',
            nr: '$nr'
          },
          total_1: { $sum: '$total_1'},
          total_2: { $sum: '$total_2'},
          car_1: { $sum: '$car_1'},
          car_2: { $sum: '$car_2'},
          truck_1: { $sum: '$truck_1'},
          truck_2: { $sum: '$truck_2'},
          bus_1: { $sum: '$bus_1'},
          bus_2: { $sum: '$bus_2'}
        }
      },
      {
        $project: {
          date: {
            $dateFromParts: {
              year: '$_id.year',
              month: '$_id.month',
              day: '$_id.day'
            }
          },
          total_1: 1,
          total_2: 1,
          car_1: 1,
          car_2: 1,
          truck_1: 1,
          truck_2: 1,
          bus_1: 1,
          bus_2: 1
        }
      },
      {
        $replaceRoot: { newRoot: { $mergeObjects: ['$_id', '$$ROOT'] } }
      },
      {
        $project: {
          _id: 0,
          year: 0,
          month: 0,
          day: 0
        }
      },
      {
        $out: 'by_day'
      }], { allowDiskUse: true });

      await db.by_day.aggregate([{
        $group: {
          _id: {
            month: { $month: '$date'},
            year: { $year: '$date'},
            name: '$name',
            land: '$land',
            type: '$type',
            roadid: '$roadid',
            lat: '$lat',
            lng: '$lng',
            nr: '$nr'
          },
          total_1: { $sum: '$total_1'},
          total_2: { $sum: '$total_2'},
          car_1: { $sum: '$car_1'},
          car_2: { $sum: '$car_2'},
          truck_1: { $sum: '$truck_1'},
          truck_2: { $sum: '$truck_2'},
          bus_1: { $sum: '$bus_1'},
          bus_2: { $sum: '$bus_2'}
        }
      },
      {
        $project: {
          date: {
            $dateFromParts: {
              month: '$_id.month',
              year: '$_id.year'
            }
          },
          total_1: 1,
          total_2: 1,
          car_1: 1,
          car_2: 1,
          truck_1: 1,
          truck_2: 1,
          bus_1: 1,
          bus_2: 1
        }
      },
      {
        $replaceRoot: { newRoot: { $mergeObjects: [ '$_id', '$$ROOT' ] } }
      },
      {
        $project: {
          _id: 0,
          month: 0,
          year: 0
        }
      },
      {
        $out: 'by_month'
      }], { allowDiskUse: true });

      await db.by_month.aggregate([{
        $group: {
          _id: {
            date: { $year: '$date'},
            name: '$name',
            land: '$land',
            type: '$type',
            roadid: '$roadid',
            lat: '$lat',
            lng: '$lng',
            nr: '$nr'
          },
          total_1: { $sum: '$total_1'},
          total_2: { $sum: '$total_2'},
          car_1: { $sum: '$car_1'},
          car_2: { $sum: '$car_2'},
          truck_1: { $sum: '$truck_1'},
          truck_2: { $sum: '$truck_2'},
          bus_1: { $sum: '$bus_1'},
          bus_2: { $sum: '$bus_2'}
        }
      },
      {
        $project: {
          date: {
            $dateFromParts: {
              year: '$_id.date'
            }
          },
          total_1: 1,
          total_2: 1,
          car_1: 1,
          car_2: 1,
          truck_1: 1,
          truck_2: 1,
          bus_1: 1,
          bus_2: 1
        }
      },
      {
        $replaceRoot: { newRoot: { $mergeObjects: [ '$_id', '$$ROOT' ] } }
      },
      {
        $project: {
          _id: 0
        }
      },
      {
        $out: 'by_year'
      }], { allowDiskUse: true });
}
