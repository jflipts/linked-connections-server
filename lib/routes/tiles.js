const util = require('util');
const fs = require('fs');
var utils = require('../utils/utils');
const tileUtils = require('../utils/tileUtils')
const Stops = require('../routes/stops')

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);

class Tiles {
    constructor() {
        this._utils = require('../utils/utils');
        this._storage = this.utils.datasetsConfig.storage;
        this._datasets = this.utils.datasetsConfig.datasets;
        this._server_config = this.utils.serverConfig;
    }

    /**
     * Returns response containing available tiles for an agency for a zoom level
     * @param {*} req
     * @param {*} res
     */
    async getAvailableTiles(req, res) {
        const agency = req.params.agency;

        // const versions = await readdir(this.storage + '/linked_pages/' + agency);
        // this.storage + '/tiles/' + companyName + '/' + zoom
        const path = this.storage + '/tiles/' + agency + '/' + req.params.zoom + '/tiles.json'

        if (fs.existsSync(path)) {
            res.set({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Content-Type': 'application/ld+json'
            });
            res.send(await readFile(path, 'utf8'));
            return;
        } else {
            // TODO Properly loop over all folders and build this way

            // const stops = await new Stops().createStopList(agency);
            // if (!fs.existsSync(this.storage + '/tiles/' + companyName + '/' + dataset.zoom)) {
            //     fs.mkdirSync(this.storage + '/tiles/' + companyName + '/' + dataset.zoom);
            // }
            // const tiles = await new Tiles().createTilesList(companyName, stops['@graph']);
            // await writeFile(path, JSON.stringify(tiles), 'utf8')

            // if (tiles != null) {
            //     res.set({
            //         'Access-Control-Allow-Origin': '*',
            //         'Access-Control-Allow-Headers': '*',
            //         'Content-Type': 'application/ld+json'
            //     });
            //     res.send(tiles);
            //     return;
            // } else {
                res.set({'Cache-Control': 'no-cache'});
                res.status(404).send("No tiles available for " + agency);
            // }
        }
    }

    /**
     * Constructs an object for an agency that in the @graph field contains the list of available tiles.
     * @param {*} company: agency, needed for data retrieval
     * @param {*} stops: list of stops, needed for computing available tiles
     */
    createTilesList(company, stops) {

        return new Promise(async (resolve, reject) => {
            let dataset = this.getDataset(company);
            let feeds = await readdir(this.storage + '/datasets/' + company);
            if (feeds.length > 0) {
                const host = (this.server_config.protocol || "http") + "://" + this.server_config.hostname;
                let skeleton = {
                    "@context": {
                        "dct": "http://purl.org/dc/terms/",
                        "schema": "http://schema.org/",
                        "name": "http://xmlns.com/foaf/0.1/name",
                        "tiles": "https://w3id.org/tree/terms#",
                        "dct:spatial": {
                            "@type": "@id"
                        },
                    },
                    "@id":  host + "/" + company + '/tiles/' + dataset.zoom,
                    "@graph": []
                };

                const tilesMap = new Map();
                stops.forEach(stop => {
                    const x = tileUtils.long2tile(parseFloat(stop['longitude']), dataset.zoom);
                    const y = tileUtils.lat2tile(parseFloat(stop['latitude']), dataset.zoom);
                    const tileId = host + "/" + company + '/connections/' + dataset.zoom + '/' + x + '/' + y;

                    tilesMap.set(tileId, {zoom: dataset.zoom, x: x, y: y})
                });

                tilesMap.forEach((value, key, _1) => {
                    skeleton['@graph'].push({
                        "@id": key,
                        "dct:spatial": dataset['geographicArea'] || "",
                        "tiles:zoom": value.zoom,
                        "tiles:longitudeTile": value.x,
                        "tiles:latitudeTile": value.y,
                    });
                });

                resolve(skeleton);
            } else {
                resolve(null);
            }
        });
    }

    getDataset(name) {
        for (let i in this.datasets) {
            if (this.datasets[i].companyName === name) {
                return this.datasets[i];
            }
        }
    }

    get utils() {
        return this._utils;
    }

    get storage() {
        return this._storage;
    }

    get datasets() {
        return this._datasets;
    }

    get server_config() {
        return this._server_config;
    }
}

module.exports = Tiles;