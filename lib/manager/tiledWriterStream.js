const Writable = require('stream').Writable;
const fs = require('fs');
const Stops = require('../routes/stops');
const tileUtils = require('../utils/tileUtils');

module.exports = class tiledWriterStream extends Writable {

  constructor(targetPath, datasetOptions) {
    super({ objectMode: true });
    this._targetPath = targetPath + '/';
    this._datasetOptions = datasetOptions;
    this._size = datasetOptions.fragmentSize || 50000;
    this._maxPageLength = datasetOptions.maxPageLength;
    this._tilingOptions = datasetOptions.tilingOptions;
    this._stopsMap = new Map();

    // Writter variables
    this._byteCounts = new Map();
    this._currentFileNames = new Map();
    this._wstreams = new Map();
    this._byteCounts = new Map();
    this._lastDepartureTimes = new Map();
    this._firstDepartureTimes = new Map();
  }

  async init() {
    // Calculate for every stop the tile coordinates
    let stops = await new Stops().createStopList(this._datasetOptions.companyName);
    stops = stops['@graph'].filter((stop) => {
      if (stop["name"].split('_').length == 1) {
        return stop;
      }
    })
    stops.forEach(stop => {
      const x = tileUtils.long2tile(parseFloat(stop['longitude']), this._tilingOptions.zoom);
      const y = tileUtils.lat2tile(parseFloat(stop['latitude']), this._tilingOptions.zoom);
      this._stopsMap.set(stop['@id'], [x, y])
    });
  }

  _write(data, encoding, done) {
    let dataString = JSON.stringify(data);
    let buffer = Buffer.from(dataString);

    const longlat = this._stopsMap.get(data['departureStop']);
    const longitude = longlat[0];
    const latitude = longlat[1];

    const key = longitude + "_" + latitude;

    const folderPath = this._targetPath + longitude + "/" + latitude + "/";

    // First connection with departureStop in this tile
    if (!this._currentFileNames.has(key)) {
      this._currentFileNames.set(key, data.departureTime);
      this._firstDepartureTimes.set(key, Date.parse(data.departureTime));
      fs.mkdirSync(folderPath, { recursive: true }, (err) => {
        if (err) throw err;
      });
      this._wstreams.set(key, fs.createWriteStream(folderPath + this._currentFileNames.get(key) + '.jsonld'));
      this._wstreams.get(key).write(dataString);
      this._byteCounts.set(key, buffer.byteLength);
    } else if ((this._byteCounts.get(key) >= this._size ||
      Date.parse(data.departureTime) - this._firstDepartureTimes.get(key) >= this._maxPageLength) &&
      data.departureTime != this._lastDepartureTimes.get(key)) {
      this._wstreams.get(key).end();

      this._currentFileNames.set(key, data.departureTime);
      this._firstDepartureTimes.set(key, Date.parse(data.departureTime));
      this._wstreams.set(key, fs.createWriteStream(folderPath + this._currentFileNames.get(key) + '.jsonld'));
      this._wstreams.get(key).write(dataString);
      this._byteCounts.set(key, buffer.byteLength);
    } else {
      this._wstreams.get(key).write(',\n' + dataString);
      this._byteCounts.set(key, this._byteCounts.get(key) + buffer.byteLength);
    }

    this._lastDepartureTimes.set(key, data.departureTime);
    done();
  }

  _final(cb) {
    if (this._wstreams.size == 0) {
      cb();
    } else {
      let finishCounter = this._wstreams.size;
      this._wstreams.forEach((stream) => {
        stream.on('finish', () => {
          if (--finishCounter <=0) {
            cb();
          }
        });
      });
    }
  }

}