const Writable = require('stream').Writable;
const fs = require('fs');

module.exports = class pageWriterStream extends Writable {

  constructor(targetPath, size, maxPageLength = Infinity) {
    super({ objectMode: true });
    this._targetPath = targetPath + '/';
    this._size = size;
    this._maxPageLenght = maxPageLength;

    this._byteCount = 0;
    this._currentFileName = '';
    this._wstream = '';

    this._lastDepartureTime = null;
    this._firstDepartureTime = null;

    fs.mkdirSync(targetPath, { recursive: true }, (err) => {
      if (err) throw err;
    });
  }

  _write(data, encoding, done) {
    let dataString = JSON.stringify(data);
    let buffer = Buffer.from(dataString);

    if (this._currentFileName == '') {
      this._currentFileName = data.departureTime;
      this._firstDepartureTime = Date.parse(data.departureTime);
      this._wstream = fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld');
      this._wstream.write(dataString);
      this._byteCount += buffer.byteLength;
    } else if ((this._byteCount >= this._size || Date.parse(data.departureTime) - this._firstDepartureTime >= this._maxPageLenght) && data.departureTime != this._lastDepartureTime) {
      this._wstream.end();
      this._currentFileName = data.departureTime;
      this._firstDepartureTime = Date.parse(data.departureTime);
      this._wstream = fs.createWriteStream(this._targetPath + this._currentFileName + '.jsonld');
      this._wstream.write(dataString);
      this._byteCount = buffer.byteLength;
    } else {
      this._wstream.write(',\n' + dataString);
      this._byteCount += buffer.byteLength;
    }

    this._lastDepartureTime = data.departureTime;
    done();
  }

  _final(cb) {
    if (this._wstream) {
      this._wstream.on('finish', () => {
        cb();
      });
      this._wstream.end();
    } else {
      cb();
    }
  }
}