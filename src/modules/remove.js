class Remove {
    constructor(wrapper, options) {
        this.wrapper = wrapper;
        this.options = options;
    }

    async remove() {
        const collection = await this.wrapper.collection(this.options.removeCollection);

        const target = Date.now() - this.options.timeout;
        if (target < 0) {
            // eslint-disable-next-line no-console
            console.error(`The timeout is incorrect.`);
            return null;
        }

        const ret = await collection.deleteMany({
            '$and': [
                { 'qoe': -1 },
                { 'start_time': { '$lte': target } }
            ]
        });
        return ret.result;
    }
};
module.exports = Remove;