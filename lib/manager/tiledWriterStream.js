const Writable = require('stream').Writable;
const fs = require('fs');

module.exports = class tiledWriterStream extends Writable {

  constructor(targetPath, size, stops) {
    super({ objectMode: true });
    this._targetPath = targetPath + '/';
    this._size = size;
    this._stops = stops;

    this._byteCounts = new Map();
    this._currentFileNames = new Map();
    this._wstreams = new Map();
    this._byteCounts = new Map();
    this._lastDepartureTimes = new Map()
  }

  _write(data, encoding, done) {
    let dataString = JSON.stringify(data);
    let buffer = Buffer.from(dataString);

    const longlat = this._stops.get(data['departureStop']);
    const longitude = longlat[0];
    const latitude = longlat[1];

    const key = longitude + "_" + latitude;

    const folderPath = this._targetPath + longitude + "/" + latitude + "/";

    // First connection with departureStop in this tile
    if (!this._currentFileNames.has(key)) {
      this._currentFileNames.set(key, data.departureTime);
      fs.mkdirSync(folderPath, { recursive: true }, (err) => {
        if (err) throw err;
      });
      this._wstreams.set(key, fs.createWriteStream(folderPath + this._currentFileNames.get(key) + '.jsonld'));
      this._wstreams.get(key).write(dataString);
      this._byteCounts.set(key, buffer.byteLength);
    } else {
      if (this._byteCounts.get(key) >= this._size && data.departureTime != this._lastDepartureTimes.get(key)) {
        this._wstreams.get(key).end();

        this._currentFileNames.set(key, data.departureTime);
        this._wstreams.set(key, fs.createWriteStream(folderPath + this._currentFileNames.get(key) + '.jsonld'));
        this._wstreams.get(key).write(dataString);
        this._byteCounts.set(key, buffer.byteLength);
      } else {
        this._wstreams.get(key).write(',\n' + dataString);
        this._byteCounts.set(key, this._byteCounts.get(key) + buffer.byteLength);
      }
    }

    this._lastDepartureTimes.set(key, data.departureTime);
    done();
  }


}