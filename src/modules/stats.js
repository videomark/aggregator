// eslint-disable-next-line import/no-unresolved
const config = require('config');

class Stats {
    constructor(wrapper, options) {
        this.wrapper = wrapper;
        this.options = options;
    }

    async stats() {
        const prefix = this.options.aggregationCollectionPrefix;
        const statsCollection = await this.wrapper.collection(this.options.statsCollection);

        const allCollectionName = `${prefix}${config.get('db.aggregation_collection_suffix.hour')}`;
        const serviceCollectionName = `${prefix}${config.get('db.aggregation_collection_suffix.service')}_${config.get('db.aggregation_collection_suffix.hour')}`;
        const countryCollectionName = `${prefix}${config.get('db.aggregation_collection_suffix.country')}_${config.get('db.aggregation_collection_suffix.hour')}`;
        const subdivisionCollectionName = `${prefix}${config.get('db.aggregation_collection_suffix.subdivision')}_${config.get('db.aggregation_collection_suffix.hour')}`;
        const ispCollectionName = `${prefix}${config.get('db.aggregation_collection_suffix.isp')}_${config.get('db.aggregation_collection_suffix.hour')}`;

        const s = {
            matched: 0,
            modified: 0,
            insert: 0
        };

        const cb = (a) => {
            s.matched += a.matchedCount;
            s.modified += a.modifiedCount;
            s.insert += a.upsertedCount;
        };

        const procs = [
            this.statsAll([allCollectionName, statsCollection, cb]),
            this.statsAllDays([allCollectionName, statsCollection, cb]),
            this.statsAllHours([allCollectionName, statsCollection, cb]),

            this.statsService([serviceCollectionName, statsCollection, cb]),
            this.statsServiceDays([serviceCollectionName, statsCollection, cb]),
            this.statsServiceHours([serviceCollectionName, statsCollection, cb]),

            this.statsCountry([countryCollectionName, statsCollection, cb]),
            this.statsCountryDays([countryCollectionName, statsCollection, cb]),
            this.statsCountryHours([countryCollectionName, statsCollection, cb]),

            this.statsSubdivision([subdivisionCollectionName, statsCollection, cb]),
            this.statsSubdivisionDays([subdivisionCollectionName, statsCollection, cb]),
            this.statsSubdivisionHours([subdivisionCollectionName, statsCollection, cb]),

            this.statsIsp([ispCollectionName, statsCollection, cb]),
            this.statsIspDays([ispCollectionName, statsCollection, cb]),
            this.statsIspHours([ispCollectionName, statsCollection, cb])
        ];

        try {
            await Promise.all(procs);
        } catch (e) {
            // eslint-disable-next-line no-console
            console.log(`stats update failed. ${e}`);
        }

        return s;
    }

    async statsAll(args) {
        const [name, dst, cb] = args;
        const src = await this.wrapper.collection(name);

        try {
            const [all] = await src.aggregate([
                {
                    '$group': {
                        '_id': null,
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'ALL',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }
            ]).toArray();

            if (!all)
                return;

            const ret = await dst.updateOne(
                {
                    '$and': [{
                        'type': 'ALL'
                    }]
                }, {
                    '$set': {
                        'min': all.min,
                        'max': all.max,
                        'average': all.average,
                        'total': all.total,
                        'count': all.count,
                    }
                }, {
                    'upsert': true
                });
            cb.call(null, ret);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsAll failed. ${err}`);
        }
    }

    async statsAllDays(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const days = await src.aggregate([
                {
                    '$project': {
                        '_id': 0,
                        'date': {
                            '$dateFromParts': {
                                'year': '$year',
                                'month': '$month',
                                'day': '$day'
                            }
                        },
                        'day': {
                            '$subtract': [{
                                '$dayOfWeek': {
                                    '$dateFromParts': {
                                        'year': '$year',
                                        'month': '$month',
                                        'day': '$day'
                                    }
                                }
                            }, 1]
                        },
                        'min': '$min',
                        'max': '$max',
                        'average': '$average',
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'day': '$day'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'ALL_DAYS',
                        'day': '$_id.day',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'day': 1
                    }
                }
            ]).toArray();

            days.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'ALL_DAYS',
                                'day': e.day
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsAllDays failed. ${err}`);
        }
    }

    async statsAllHours(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const hours = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'hour': '$hour'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'ALL_HOURS',
                        'hour': '$_id.hour',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'hour': 1
                    }
                }
            ]).toArray();

            hours.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'ALL_HOURS',
                                'hour': e.hour
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsAllHours failed. ${err}`);
        }
    }

    async statsService(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const all = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'service': '$service'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'SERVICE',
                        'service': '$_id.service',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }
            ]).toArray();

            all.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'SERVICE',
                                'service': e.service
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsService failed. ${err}`);
        }
    }

    async statsServiceDays(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const days = await src.aggregate([
                {
                    '$project': {
                        '_id': 0,
                        'date': {
                            '$dateFromParts': {
                                'year': '$year',
                                'month': '$month',
                                'day': '$day'
                            }
                        },
                        'day': {
                            '$subtract': [{
                                '$dayOfWeek': {
                                    '$dateFromParts': {
                                        'year': '$year',
                                        'month': '$month',
                                        'day': '$day'
                                    }
                                }
                            }, 1]
                        },
                        'service': '$service',
                        'min': '$min',
                        'max': '$max',
                        'average': '$average',
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'service': '$service',
                            'day': '$day'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'SERVICE_DAYS',
                        'service': '$_id.service',
                        'day': '$_id.day',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'day': 1
                    }
                }
            ]).toArray();

            days.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'SERVICE_DAYS',
                                'service': e.service,
                                'day': e.day
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsServiceDays failed. ${err}`);
        }
    }

    async statsServiceHours(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const hours = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'service': '$service',
                            'hour': '$hour'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'SERVICE_HOURS',
                        'service': '$_id.service',
                        'hour': '$_id.hour',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'hour': 1
                    }
                }
            ]).toArray();

            hours.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'SERVICE_HOURS',
                                'service': e.service,
                                'hour': e.hour
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsServiceHours failed. ${err}`);
        }
    }

    async statsCountry(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const all = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'country': '$country'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'COUNTRY',
                        'country': '$_id.country',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }
            ]).toArray();

            all.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'COUNTRY',
                                'country': e.country
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsCountry failed. ${err}`);
        }
    }

    async statsCountryDays(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const days = await src.aggregate([
                {
                    '$project': {
                        '_id': 0,
                        'date': {
                            '$dateFromParts': {
                                'year': '$year',
                                'month': '$month',
                                'day': '$day'
                            }
                        },
                        'day': {
                            '$subtract': [{
                                '$dayOfWeek': {
                                    '$dateFromParts': {
                                        'year': '$year',
                                        'month': '$month',
                                        'day': '$day'
                                    }
                                }
                            }, 1]
                        },
                        'country': '$country',
                        'min': '$min',
                        'max': '$max',
                        'average': '$average',
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'country': '$country',
                            'day': '$day'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'COUNTRY_DAYS',
                        'country': '$_id.country',
                        'day': '$_id.day',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'day': 1
                    }
                }
            ]).toArray();

            days.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'COUNTRY_DAYS',
                                'country': e.country,
                                'day': e.day
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsCountryDays failed. ${err}`);
        }
    }

    async statsCountryHours(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const hours = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'country': '$country',
                            'hour': '$hour'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'COUNTRY_HOURS',
                        'country': '$_id.country',
                        'hour': '$_id.hour',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'hour': 1
                    }
                }
            ]).toArray();

            hours.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'COUNTRY_HOURS',
                                'country': e.country,
                                'hour': e.hour
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsCountryHours failed. ${err}`);
        }
    }

    async statsSubdivision(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const all = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'country': '$country',
                            'subdivision': '$subdivision'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'SUBDIVISION',
                        'country': '$_id.country',
                        'subdivision': '$_id.subdivision',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }
            ]).toArray();

            all.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'SUBDIVISION',
                                'country': e.country,
                                'subdivision': e.subdivision
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsSubdivision failed. ${err}`);
        }
    }

    async statsSubdivisionDays(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const days = await src.aggregate([
                {
                    '$project': {
                        '_id': 0,
                        'date': {
                            '$dateFromParts': {
                                'year': '$year',
                                'month': '$month',
                                'day': '$day'
                            }
                        },
                        'day': {
                            '$subtract': [{
                                '$dayOfWeek': {
                                    '$dateFromParts': {
                                        'year': '$year',
                                        'month': '$month',
                                        'day': '$day'
                                    }
                                }
                            }, 1]
                        },
                        'country': '$country',
                        'subdivision': '$subdivision',
                        'min': '$min',
                        'max': '$max',
                        'average': '$average',
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'country': '$country',
                            'subdivision': '$subdivision',
                            'day': '$day'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'SUBDIVISION_DAYS',
                        'country': '$_id.country',
                        'subdivision': '$_id.subdivision',
                        'day': '$_id.day',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'day': 1
                    }
                }
            ]).toArray();

            days.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'SUBDIVISION_DAYS',
                                'country': e.country,
                                'subdivision': e.subdivision,
                                'day': e.day
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsSubdivisionDays failed. ${err}`);
        }
    }

    async statsSubdivisionHours(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const hours = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'country': '$country',
                            'subdivision': '$subdivision',
                            'hour': '$hour'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'SUBDIVISION_HOURS',
                        'country': '$_id.country',
                        'subdivision': '$_id.subdivision',
                        'hour': '$_id.hour',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'hour': 1
                    }
                }
            ]).toArray();

            hours.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'SUBDIVISION_HOURS',
                                'country': e.country,
                                'subdivision': e.subdivision,
                                'hour': e.hour
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsSubdivisionHours failed. ${err}`);
        }
    }

    async statsIsp(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const all = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'isp': '$isp'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'ISP',
                        'isp': '$_id.isp',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }
            ]).toArray();

            all.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'ISP',
                                'isp': e.isp
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsIsp failed. ${err}`);
        }
    }

    async statsIspDays(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const days = await src.aggregate([
                {
                    '$project': {
                        '_id': 0,
                        'date': {
                            '$dateFromParts': {
                                'year': '$year',
                                'month': '$month',
                                'day': '$day'
                            }
                        },
                        'day': {
                            '$subtract': [{
                                '$dayOfWeek': {
                                    '$dateFromParts': {
                                        'year': '$year',
                                        'month': '$month',
                                        'day': '$day'
                                    }
                                }
                            }, 1]
                        },
                        'isp': '$isp',
                        'min': '$min',
                        'max': '$max',
                        'average': '$average',
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'isp': '$isp',
                            'day': '$day'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'ISP_DAYS',
                        'isp': '$_id.isp',
                        'day': '$_id.day',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'day': 1
                    }
                }
            ]).toArray();

            days.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'ISP_DAYS',
                                'isp': e.isp,
                                'day': e.day
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsIspDays failed. ${err}`);
        }
    }

    async statsIspHours(args) {
        const [name, dst, cb] = args;
        const update = [];
        const stats = {
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0
        };

        const src = await this.wrapper.collection(name);

        try {
            const hours = await src.aggregate([
                {
                    '$group': {
                        '_id': {
                            'isp': '$isp',
                            'hour': '$hour'
                        },
                        'min': { '$min': '$min' },
                        'max': { '$max': '$max' },
                        'total': { '$sum': '$total' },
                        'count': { '$sum': '$count' }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'type': 'ISP_HOURS',
                        'isp': '$_id.isp',
                        'hour': '$_id.hour',
                        'min': '$min',
                        'max': '$max',
                        'average': {
                            '$divide': ['$total', '$count']
                        },
                        'total': '$total',
                        'count': '$count'
                    }
                }, {
                    '$sort': {
                        'hour': 1
                    }
                }
            ]).toArray();

            hours.forEach(e => {
                update.push((async () => {
                    const ret = await dst.updateOne(
                        {
                            '$and': [{
                                'type': 'ISP_HOURS',
                                'isp': e.isp,
                                'hour': e.hour
                            }]
                        }, {
                            '$set': {
                                'min': e.min,
                                'max': e.max,
                                'average': e.average,
                                'total': e.total,
                                'count': e.count,
                            }
                        }, {
                            'upsert': true
                        });
                    stats.matchedCount += ret.matchedCount;
                    stats.modifiedCount += ret.modifiedCount;
                    stats.upsertedCount += ret.upsertedCount;
                })());
            });

            await Promise.all(update);

            cb.call(null, stats);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error(`statsIspHours failed. ${err}`);
        }
    }

    async remove() {
        try {
            const collection = await this.wrapper.collection(this.options.statsCollection);
            collection.deleteMany({});
        } catch (e) {
            // eslint-disable-next-line no-console
            console.error(`failed to remove ${e}`);
        }
    }
};
module.exports = Stats;