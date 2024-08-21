#!/usr/bin/env node

/* eslint-disable no-console */

const { program } = require("commander");
// eslint-disable-next-line import/no-unresolved
const config = require("config");
// eslint-disable-next-line import/no-unresolved
const moment = require("moment");

// Modules
// eslint-disable-next-line import/no-unresolved
const { MongoWrapper, LocationFinder, ISPFinder } = require("qoe-lib");

const Daemon = require("./src/modules/daemon");
const Update = require("./src/modules/update");
const Insert = require("./src/modules/insert");
const Remove = require("./src/modules/remove");
const Stats = require("./src/modules/stats");

const { version } = require("./package.json");

const MODE_NAME_DAEMON = "daemon";
const MODE_NAME_UPDATE = "update";
const MODE_NAME_INSERT = "insert";
const MODE_NAME_REMOVE = "remove";
const MODE_NAME_STATS = "stats";

const options = {};

const dumpOptions = () => {
  //  console.log(JSON.stringify(option))
  console.log(`=== OPTIONS ===`);
  console.log(`   Execute mode: ${options.mode}`);
  console.log(``);
  console.log(`   DB`);
  console.log(`     type  : ${options.db_type}`);
  console.log(`     url   : ${options.db_url}`);
  console.log(`     name  : ${options.db_name}`);
  console.log(``);
  if (options.mode === MODE_NAME_DAEMON) {
    console.log(`  --- DAEMON MODE ---`);
    console.log(``);
    console.log(`   Interval`);
    console.log(`     aggregate : ${options.aggregateInterval}`);
    //  console.log(`     update    : ${options.updateInterval}`);
    console.log(`     insert    : ${options.insertInterval}`);
    //  console.log(`     remove    : ${options.removeInterval}`);
    console.log(`     stats     : ${options.statsInterval}`);
    console.log(``);
    console.log(`   Collection`);
    console.log(`     Source              : ${options.sourceCollection}`);
    console.log(
      `     Destination Prefix  : ${options.aggregationCollectionPrefix}`
    );
    console.log(`     Stats               : ${options.statsCollection}`);
    //  console.log(``);
    //  console.log(`   Files`);
    //  console.log(`     City  Path(maxmind) : ${options.cityFile}`);
    //  console.log(`     ISP   Path(maxmind) : ${options.ispFile}`);
    //  console.log(``);
    //  console.log(`   Remove`);
    //  console.log(`     Timeout: ${options.timeout}`);
  } else if (options.mode === MODE_NAME_UPDATE) {
    console.log(`  --- UPDATE MODE ---`);
    console.log(`   Collection`);
    console.log(`     Source: ${options.sourceCollection}`);
    console.log(``);
    console.log(`   Start   Time  : ${options.startTime}`);
    console.log(`   End     Time  : ${options.endTime}`);
    console.log(``);
    console.log(`   Files`);
    console.log(`     City  Path(maxmind) : ${options.cityFile}`);
    console.log(`     ISP   Path(maxmind) : ${options.ispFile}`);
    console.log(``);
    console.log(`   Remove Fields option: ${options.removeFields}`);
  } else if (options.mode === MODE_NAME_INSERT) {
    console.log(`  --- INSERT MODE ---`);
    console.log(`   Collection`);
    console.log(`     Source              : ${options.sourceCollection}`);
    console.log(
      `     Destination Prefix  : ${options.aggregationCollectionPrefix}`
    );
    console.log(``);
    console.log(`   Start   Time  : ${options.startTime}`);
    console.log(`   End     Time  : ${options.endTime}`);
    console.log(``);
    console.log(
      `   Remove Aggregation Collection option: ${options.removeAggregationCollection}`
    );
  } else if (options.mode === MODE_NAME_REMOVE) {
    console.log(`  --- REMOVE MODE ---`);
    console.log(`   Collection`);
    console.log(`     Remove entry of timeout: ${options.removeCollection}`);
    console.log(``);
    console.log(`   Timeout: ${options.timeout}`);
  } else if (options.mode === MODE_NAME_STATS) {
    console.log(`  --- STATS MODE ---`);
    console.log(`   Collection`);
    console.log(`     Stats: ${options.statsCollection}`);
    console.log(
      `     Aggregate Prefix  : ${options.aggregationCollectionPrefix}`
    );
    console.log(``);
    console.log(
      `   Remove Stats Collection option: ${options.removeStatsCollection}`
    );
  }
  console.log(``);
};

const startEndDateParse = (o) => {
  try {
    if (o.start_time)
      options.startTime = moment(o.start_time, moment.ISO_8601).toDate();
    else options.startTime = new Date(0);
  } catch (e) {
    options.startTime = new Date(0);
    console.error(`${e}, format error use default value`);
  }
  try {
    if (o.end_time)
      options.endTime = moment(o.end_time, moment.ISO_8601).toDate();
    else options.endTime = new Date();
  } catch (e) {
    options.endTime = new Date();
    console.error(`${e}, format error use default value`);
  }
};

/**
 * common options
 */
program
  .version(version, "-v --version")
  .option("-t --db_type [type]", `DB Type default[${config.get("db.type")}]`)
  .option("-u --db_url [url]", `MongoDB URL default[${config.get("db.url")}]`)
  .option("-n --db_name [name]", `DB Type default[${config.get("db.name")}]`)
  .usage("$ [mode]");

/**
 * daemon mode options
 */
program
  .command(MODE_NAME_DAEMON)
  .description(
    `${MODE_NAME_DAEMON} mode: monitors the DB, adds fields, and performs aggregation.`
  )
  .option(
    "-c --source_collection [collection_name]",
    `Name of collection that storing from Extension default[${config.get(
      "db.source_collection"
    )}]`
  )
  .option(
    "   --aggregation_collection_prefix [prefix]",
    `prefix of aggregation collection name. default[${config.get(
      "db.aggregation_collection_prefix"
    )}]`
  )
  //  .option('   --remove_collection [collection_name]', `Name of collection to remove timeout entry. default[${config.get('db.source_collection')}]`)
  .option(
    "   --stats_collection [collection_name]",
    `Name of collection for stats. default[${config.get(
      "db.stats_collection"
    )}]`
  )
  .option(
    "   --aggregate_interval [millisec]",
    `aggregate interval default[${config.get("interval.aggregate")}]`
  )
  //  .option('   --update_interval [millisec]', `DB update interval default[${config.get('interval.update')}]`)
  .option(
    "   --insert_interval [millisec]",
    `DB insert interval default[${config.get("interval.insert")}]`
  )
  //  .option('   --remove_interval [millisec]', `DB remove interval default[${config.get('interval.remove')}]`)
  .option(
    "   --stats_interval [millisec]",
    `DB stats interval default[${config.get("interval.stats")}]`
  )
  //  .option('   --city_file [city_file]', `Maxmind city file default[${config.get('file.city')}]`)
  //  .option('   --isp_file [isp_file]', `Maxmind ISP file default[${config.get('file.ISP')}]`)
  .option(
    "-t --timeout [MILLISECOND]",
    `QoE Server timeout. default ${
      config.get("qoe.timeout") / (60 * 60 * 1000)
    }h [${config.get("qoe.timeout")}]`
  )
  .alias("d")
  .action((o) => {
    options.mode = MODE_NAME_DAEMON;
    options.sourceCollection =
      o.source_collection || config.get("db.source_collection");
    options.aggregationCollectionPrefix =
      o.aggregation_collection_prefix ||
      config.get("db.aggregation_collection_prefix");
    options.removeCollection =
      o.remove_collection || config.get("db.source_collection");
    options.statsCollection =
      o.stats_collection || config.get("db.stats_collection");

    options.aggregateInterval =
      o.aggregate_interval || config.get("interval.aggregate");
    //  options.updateInterval = o.update_interval || config.get('interval.update');
    options.insertInterval = o.insert_interval || config.get("interval.insert");
    options.removeInterval = o.remove_interval || config.get("interval.remove");
    options.statsInterval = o.stats_interval || config.get("interval.stats");

    //  options.cityFile = o.city_file || config.get('file.city');
    //  options.ispFile = o.isp_file || config.get('file.ISP');

    options.timeout = o.timeout || config.get("qoe.timeout");
  });

/**
 * update mode options
 */
program
  .command(MODE_NAME_UPDATE)
  .description(
    `${MODE_NAME_UPDATE} mode: generate location, isp, service type field`
  )
  .option(
    "-c --source_collection [collection_name]",
    `Name of collection that storing from Extension default[${config.get(
      "db.source_collection"
    )}]`
  )
  .option(
    "-s --start_time [DATE_TIME]",
    `start_time filed query(gte) for update. format ISO_8601 defalut[1970-01-01T09:00:00+09:00]`
  )
  .option(
    "-e --end_time [DATE_TIME]",
    `start_time filed query(lt) for update. format ISO_8601 defalut[now]`
  )
  .option(
    "   --city_file [city_file]",
    `Maxmind city file default[${config.get("file.city")}]`
  )
  .option(
    "   --isp_file [isp_file]",
    `Maxmind ISP file default[${config.get("file.ISP")}]`
  )
  .option("   --remove_fields", `remove update fields default[false]`)
  .alias("u")
  .action((o) => {
    options.mode = MODE_NAME_UPDATE;
    options.sourceCollection =
      o.source_collection || config.get("db.source_collection");
    options.aggregationCollectionPrefix =
      o.aggregation_collection_prefix ||
      config.get("db.aggregation_collection_prefix");
    startEndDateParse(o);
    options.cityFile = o.city_file || config.get("file.city");
    options.ispFile = o.isp_file || config.get("file.ISP");
    options.removeFields = o.remove_fields || false;
  });

/**
 * insert mode options
 */
program
  .command(MODE_NAME_INSERT)
  .description(`${MODE_NAME_INSERT} mode: aggregation value insert into DB`)
  .option(
    "-c --source_collection [collection_name]",
    `Name of collection to aggregation. default[${config.get(
      "db.source_collection"
    )}]`
  )
  .option(
    "-s --start_time [DATE_TIME]",
    `start_time filed query(gte) for aggregation. format ISO_8601 defalut[1970-01-01T09:00:00+09:00]`
  )
  .option(
    "-e --end_time [DATE_TIME]",
    `start_time filed query(lt) for aggregation. format ISO_8601 defalut[now]`
  )
  .option(
    "   --aggregation_collection_prefix [prefix]",
    `prefix of aggregation collection name. default[${config.get(
      "db.aggregation_collection_prefix"
    )}]`
  )
  .option(
    "   --remove_aggregation_collection",
    `remove aggregation collection. default[false]`
  )
  .alias("i")
  .action((o) => {
    options.mode = MODE_NAME_INSERT;
    options.sourceCollection =
      o.source_collection || config.get("db.source_collection");
    startEndDateParse(o);
    options.aggregationCollectionPrefix =
      o.aggregation_collection_prefix ||
      config.get("db.aggregation_collection_prefix");
    options.removeAggregationCollection =
      o.remove_aggregation_collection || false;
  });

/**
 * remove mode options
 */
program
  .command(MODE_NAME_REMOVE)
  .description(`${MODE_NAME_REMOVE} mode: remove timeout entry from DB`)
  .option(
    "-t --timeout [MILLISECOND]",
    `QoE Server timeout. default ${
      config.get("qoe.timeout") / (60 * 60 * 1000)
    }h [${config.get("qoe.timeout")}]`
  )
  .option(
    "   --remove_collection [collection_name]",
    `Name of collection to remove timeout entry. default[${config.get(
      "db.source_collection"
    )}]`
  )
  .alias("r")
  .action((o) => {
    options.mode = MODE_NAME_REMOVE;
    options.removeCollection =
      o.remove_collection || config.get("db.source_collection");
    options.timeout = o.timeout || config.get("qoe.timeout");
  });

/**
 * stats mode options
 */
program
  .command(MODE_NAME_STATS)
  .description(`${MODE_NAME_STATS} mode: make current stats from DB`)
  .option(
    "   --stats_collection [collection_name]",
    `Name of collection. default[${config.get("db.stats_collection")}]`
  )
  .option(
    "   --aggregation_collection_prefix [prefix]",
    `prefix of aggregation collection name. default[${config.get(
      "db.aggregation_collection_prefix"
    )}]`
  )
  .option(
    "   --remove_stats_collection",
    `remove stats collection. default[false]`
  )
  .alias("s")
  .action((o) => {
    options.mode = MODE_NAME_STATS;
    options.statsCollection =
      o.stats_collection || config.get("db.stats_collection");
    options.aggregationCollectionPrefix =
      o.aggregation_collection_prefix ||
      config.get("db.aggregation_collection_prefix");
    options.removeStatsCollection = o.remove_stats_collection || false;
  });

// --- options --- //
program.parse(process.argv);
// NOTE: commander v7 以降コマンドのプロパティとして保存されないのでその対処
Object.assign(program, program.opts());

if (program.args.length === 0 || !options.mode) {
  program.help();
}

options.db_type = program.db_type || config.get("db.type");
options.db_url = program.db_url || config.get("db.url");
options.db_name = program.db_name || config.get("db.name");

dumpOptions();

// --- main --- //
(async () => {
  let mongodb;
  try {
    mongodb = new MongoWrapper(options.db_url, options.db_name);
    await mongodb.open();
  } catch (e) {
    console.error(`failed to connection ${e}`);
    return;
  }

  if (options.mode === MODE_NAME_DAEMON) {
    const daemon = new Daemon(mongodb, options);
    daemon.start();
  } else if (options.mode === MODE_NAME_UPDATE) {
    const finder = {
      location: new LocationFinder(options.cityFile),
      isp: new ISPFinder(options.ispFile),
    };

    const update = new Update(mongodb, finder, options);

    if (options.removeFields) {
      await update.remove(options.startTime, options.endTime);
    } else {
      const stats = await update.update(options.startTime, options.endTime);
      // eslint-disable-next-line no-console
      console.log(
        `${stats.matched} documents matched, ${stats.modified} documents added country, subdivision, isp and service fields, ${stats.insert} documents inserted.`
      );
    }

    mongodb.close();
  } else if (options.mode === MODE_NAME_INSERT) {
    const insert = new Insert(mongodb, options);

    if (options.removeAggregationCollection) {
      await insert.remove();
    } else {
      const stats = await insert.insert(options.startTime, options.endTime);
      console.log(
        `${stats.matched} documents matched, ${stats.modified} documents updatede, ${stats.insert} documents inserted.`
      );
    }

    mongodb.close();
  } else if (options.mode === MODE_NAME_REMOVE) {
    const remove = new Remove(mongodb, options);

    const stats = await remove.remove();
    // eslint-disable-next-line no-console
    console.log(`${stats.n} documents deleted`);
    mongodb.close();
  } else if (options.mode === MODE_NAME_STATS) {
    const s = new Stats(mongodb, options);

    if (options.removeStatsCollection) {
      await s.remove();
    } else {
      const stats = await s.stats();
      console.log(
        `${stats.matched} documents matched, ${stats.modified} documents updatede, ${stats.insert} documents inserted.`
      );
    }
    mongodb.close();
  }
})();
