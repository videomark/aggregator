// eslint-disable-next-line import/no-unresolved
// const { ServiceName } = require('qoe-lib');

class Update {
  constructor(wrapper, finder, options) {
    this.wrapper = wrapper;
    this.finder = finder;
    this.options = options;
  }

  /**
   * add service, country, subdivision, isp field.
   * @param {Date} start
   * @param {Date} end
   */
  async update(start = new Date(0), end = new Date()) {
    if (
      !start ||
      !end ||
      !(start instanceof Date) ||
      !(end instanceof Date) ||
      start.getTime() > end.getTime()
    ) {
      return null;
    }

    const collection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const values = await collection
      .find({
        $and: [
          {
            start_time: {
              $gte: start.getTime(),
              $lt: end.getTime(),
            },
          },
          // { 'service': { '$exists': false } },
          { country: { $exists: false } },
          { subdivision: { $exists: false } },
          { isp: { $exists: false } },
        ],
      })
      .toArray();

    const update = [];
    const stats = {
      matched: 0,
      modified: 0,
      insert: 0,
    };

    values.forEach((e) => {
      if (e.remote_address && e.location) {
        update.push(
          (async () => {
            const { country, subdivision } = this.finder.location.find(
              e.remote_address
            );
            const isp = await this.finder.isp.find(e.remote_address);
            // const service = ServiceName.find(e.location);
            const ret = await collection.updateOne(
              {
                // eslint-disable-next-line no-underscore-dangle
                _id: e._id,
              },
              {
                $set: {
                  country: country,
                  subdivision: subdivision,
                  isp: isp,
                  // 'service': service
                },
              },
              {
                upsert: false,
              }
            );
            stats.matched += ret.matchedCount;
            stats.modified += ret.modifiedCount;
            stats.insert += ret.upsertedCount;
          })()
        );
      }
    });

    try {
      await Promise.all(update);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(`updateOne failed. ${err}`);
    }
    return stats;
  }

  /**
   * remove service, country, subdivision, isp field
   * @param {Date} start
   * @param {Date} end
   */
  async remove(start = new Date(0), end = new Date()) {
    if (
      !start ||
      !end ||
      !(start instanceof Date) ||
      !(end instanceof Date) ||
      start.getDate() > end.getDate()
    ) {
      return;
    }

    const collection = await this.wrapper.collection(
      this.options.sourceCollection
    );
    const values = await collection
      .find({
        $and: [
          {
            start_time: {
              $gte: start.getTime(),
              $lt: end.getTime(),
            },
          },
          {
            remote_address: { $exists: true },
          },
        ],
      })
      .toArray();

    values.forEach((e) => {
      if (e.remote_address && e.location) {
        collection.updateOne(
          {
            // eslint-disable-next-line no-underscore-dangle
            _id: e._id,
          },
          {
            $unset: {
              country: "",
              subdivision: "",
              city: "",
              isp: "",
              // 'service': '',
            },
          },
          {
            upsert: false,
          }
        );
      }
    });
  }
}
module.exports = Update;
