// eslint-disable-next-line import/no-unresolved
const moment = require('moment')

const Insert = require('./insert');
const Stats = require('./stats');

class Daemon {
    constructor(wrapper, options) {
        this.wrapper = wrapper;
        this.options = options;

        this.insert = new Insert(this.wrapper, this.options);
        this.stats = new Stats(this.wrapper, this.options);

        this.insertTimer = null;
        this.statsTimer = null;

        this.lastInserted = null;
    }

    start() {
        this.insertTimer = setInterval(async () => this.insertProc(), this.options.insertInterval);
        this.statsTimer = setInterval(async () => this.statsProc(), this.options.statsInterval);
    }

    async insertProc() {
        const to = new Date();

        let from;
        if (this.lastInserted)
            from = new Date(Math.floor((to.getTime() - this.options.aggregateInterval) / (1000 * 60 * 60)) * 1000 * 60 * 60);
        else
            from = new Date(0);

        const stats = await this.insert.insert(from, to);

        this.lastInserted = from;

        // eslint-disable-next-line no-console
        console.log(`Insert ${stats.matched} documents matched, ${stats.modified} documents updated, ${stats.insert} documents inserted. [${
            moment(from).format()} - ${moment(to).format()}]`);
    }

    async statsProc() {
        const now = new Date();
        const stats = await this.stats.stats();
        // eslint-disable-next-line no-console
        console.log(`Stats ${stats.matched} documents matched, ${stats.modified} documents updated, ${stats.insert} documents inserted. [${
            moment(now).format()}]`);
    }
};
module.exports = Daemon;