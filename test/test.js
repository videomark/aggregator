/* eslint-env mocha */

const assert = require("assert");

// eslint-disable-next-line import/no-unresolved
const config = require("config");

// eslint-disable-next-line import/no-unresolved
const { LocationFinder, ISPFinder, ServiceName } = require("qoe-lib");

describe("Aggregator Test", () => {
  it("LocationFinder normal:", async () => {
    const address = "182.171.253.140";

    const v = {
      country: "JP",
      subdivision: "13",
    };
    const finder = new LocationFinder(config.get("file.city"));
    const r = await finder.find(address);
    assert.equal(v.country, r.country);
    assert.equal(v.subdivision, r.subdivision);
  });

  it("LocationFinder null:", async () => {
    const address = "172.27.200.113";

    const v = {
      country: "--",
      subdivision: "0",
    };
    const finder = new LocationFinder(config.get("file.city"));
    const r = await finder.find(address);
    assert.equal(v.country, r.country);
    assert.equal(v.subdivision, r.subdivision);
  });

  it("LocationFinder sub null:", async () => {
    const address = "150.66.69.159";

    const v = {
      country: "JP",
      subdivision: "0",
    };
    const finder = new LocationFinder(config.get("file.city"));
    const r = await finder.find(address);
    assert.equal(v.country, r.country);
    assert.equal(v.subdivision, r.subdivision);
  });

  it("ISPFinder normal", async () => {
    const address = "182.171.253.140";

    const v = "So-net";
    const finder = new ISPFinder(config.get("file.ISP"));
    const r = await finder.find(address);
    assert.equal(r, v);
  });

  it("ServiceName normal", () => {
    const hosts = [
      "https://www.youtube.com/watch?v=wCDIYvFmgW8",
      "https://m.youtube.com/watch?v=q7fDTjZ9hJY",
      "https://www.paravi.jp/watch/10534",
      "https://tver.jp/corner/f0028338",
      "http://pr.iij.ad.jp/live/",
      "https://docs.mongodb.com/manual",
    ];

    const v = [
      "youtube",
      "youtube",
      "paravi",
      "tver",
      "iijtwilightconcert",
      "unknown",
    ];

    hosts.forEach((e, i) => {
      assert.equal(v[i], ServiceName.find(e));
    });
  });
});
