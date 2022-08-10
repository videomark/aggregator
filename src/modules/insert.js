// eslint-disable-next-line import/no-unresolved
const config = require("config");

class Insert {
  constructor(wrapper, options) {
    this.wrapper = wrapper;
    this.options = options;
  }

  async insert(start = new Date(0), end = new Date()) {
    if (
      !start ||
      !end ||
      !(start instanceof Date) ||
      !(end instanceof Date) ||
      start.getTime() > end.getTime()
    ) {
      return null;
    }

    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    const values = [];

    values.push(await this.insertAggrDay(start, end));
    values.push(await this.insertAggrHour(start, end));

    values.push(await this.insertAggrServiceDay(start, end));
    values.push(await this.insertAggrServiceHour(start, end));

    values.push(await this.insertAggrCountryDay(start, end));
    values.push(await this.insertAggrCountryHour(start, end));

    values.push(await this.insertAggrSubdivisionDay(start, end));
    values.push(await this.insertAggrSubdivisionHour(start, end));

    values.push(await this.insertAggrISPDay(start, end));
    values.push(await this.insertAggrISPHour(start, end));

    values.forEach((e) => {
      stats.matched += e.matched;
      stats.modified += e.modified;
      stats.insert += e.insert;
    });

    return stats;
  }

  async insertAggrDay(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.day"
      )}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            day: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrHour(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.hour"
      )}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              hour: "$datetime.hour",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            hour: "$_id.hour",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            hour: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  hour: e.hour,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrServiceDay(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.service"
      )}_${config.get("db.aggregation_collection_suffix.day")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              service: "$service",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            service: "$_id.service",
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            day: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  service: e.service,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrServiceHour(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.service"
      )}_${config.get("db.aggregation_collection_suffix.hour")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              hour: "$datetime.hour",
              service: "$service",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            hour: "$_id.hour",
            service: "$_id.service",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            hour: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  hour: e.hour,
                  service: e.service,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrCountryDay(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.country"
      )}_${config.get("db.aggregation_collection_suffix.day")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              country: "$country",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            country: "$_id.country",
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            day: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  country: e.country,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrCountryHour(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.country"
      )}_${config.get("db.aggregation_collection_suffix.hour")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              hour: "$datetime.hour",
              country: "$country",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            hour: "$_id.hour",
            country: "$_id.country",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            hour: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  hour: e.hour,
                  country: e.country,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrSubdivisionDay(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.subdivision"
      )}_${config.get("db.aggregation_collection_suffix.day")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              country: "$country",
              subdivision: "$subdivision",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            country: "$_id.country",
            subdivision: "$_id.subdivision",
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            day: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  country: e.country,
                  subdivision: e.subdivision,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrSubdivisionHour(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.subdivision"
      )}_${config.get("db.aggregation_collection_suffix.hour")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              hour: "$datetime.hour",
              country: "$country",
              subdivision: "$subdivision",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            hour: "$_id.hour",
            country: "$_id.country",
            subdivision: "$_id.subdivision",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            hour: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  hour: e.hour,
                  country: e.country,
                  subdivision: e.subdivision,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrISPDay(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.isp"
      )}_${config.get("db.aggregation_collection_suffix.day")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              isp: "$isp",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            isp: "$_id.isp",
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            day: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  isp: e.isp,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async insertAggrISPHour(start, end) {
    const srcCollection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const dstCollection = await this.wrapper.collection(
      `${this.options.aggregationCollectionPrefix}${config.get(
        "db.aggregation_collection_suffix.isp"
      )}_${config.get("db.aggregation_collection_suffix.hour")}`
    );

    const values = await srcCollection
      .aggregate([
        {
          $match: {
            $and: [
              { qoe: { $gte: 0.0 } },
              { qoe: { $lte: 5.0 } },
              {
                start_time: {
                  $gte: start.getTime(),
                  $lt: end.getTime(),
                },
              },
            ],
          },
        },
        {
          $project: {
            _id: 0,
            datetime: {
              $dateToParts: {
                date: { $toDate: "$start_time" },
                timezone: "Asia/Tokyo",
              },
            },
            service: "$service",
            country: "$country",
            subdivision: "$subdivision",
            isp: "$isp",
            qoe: "$qoe",
          },
        },
        {
          $group: {
            _id: {
              year: "$datetime.year",
              month: "$datetime.month",
              day: "$datetime.day",
              hour: "$datetime.hour",
              isp: "$isp",
            },
            min: { $min: "$qoe" },
            max: { $max: "$qoe" },
            average: { $avg: "$qoe" },
            stdDevPop: { $stdDevPop: "$qoe" },
            total: { $sum: "$qoe" },
            count: { $sum: 1 },
          },
        },
        {
          $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            day: "$_id.day",
            hour: "$_id.hour",
            isp: "$_id.isp",
            min: 1,
            max: 1,
            average: 1,
            stdDevPop: 1,
            total: 1,
            count: 1,
            timezone: "Asia/Tokyo",
          },
        },
        {
          $sort: {
            hour: 1,
          },
        },
      ])
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      update.push(
        (async () => {
          const ret = await dstCollection.updateOne(
            {
              $and: [
                {
                  year: e.year,
                  month: e.month,
                  day: e.day,
                  hour: e.hour,
                  isp: e.isp,
                },
              ],
            },
            {
              $set: {
                min: e.min,
                max: e.max,
                average: e.average,
                stdDevPop: e.stdDevPop,
                total: e.total,
                count: e.count,
              },
            },
            {
              upsert: true,
            }
          );
          stats.matched += ret.matchedCount;
          stats.modified += ret.modifiedCount;
          stats.insert += ret.upsertedCount;
        })()
      );
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  async remove() {
    const collections = [];
    try {
      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.day"
          )}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.hour"
          )}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.service"
          )}_${config.get("db.aggregation_collection_suffix.day")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.service"
          )}_${config.get("db.aggregation_collection_suffix.hour")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.country"
          )}_${config.get("db.aggregation_collection_suffix.day")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.country"
          )}_${config.get("db.aggregation_collection_suffix.hour")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.subdivision"
          )}_${config.get("db.aggregation_collection_suffix.day")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.subdivision"
          )}_${config.get("db.aggregation_collection_suffix.hour")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.isp"
          )}_${config.get("db.aggregation_collection_suffix.day")}`
        )
      );

      collections.push(
        await this.wrapper.collection(
          `${this.options.aggregationCollectionPrefix}${config.get(
            "db.aggregation_collection_suffix.isp"
          )}_${config.get("db.aggregation_collection_suffix.hour")}`
        )
      );
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error(`failed to collection ${e}`);
    }

    for (let i = 0; i < collections.length; i += 1) {
      try {
        collections[i].deleteMany({});
      } catch (e) {
        // eslint-disable-next-line no-console
        console.error(`failed to remove ${e}`);
      }
    }
  }
}
module.exports = Insert;
