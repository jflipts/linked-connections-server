const util = require('util');
const fs = require('fs');
var utils = require('../utils/utils');
const tileUtils = require('../utils/tileUtils')
const path = require('path');
const Logger = require('../utils/logger');

const writeFile = util.promisify(fs.writeFile);
const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);

/**
 * Class for /:agency/tiles endpoint
 */
class Tiles {
    constructor() {
        this._utils = require('../utils/utils');
        this._storage = this.utils.datasetsConfig.storage;
        this._datasets = this.utils.datasetsConfig.datasets;
        this._server_config = this.utils.serverConfig;
    }

    /**
     * Returns response containing available tiles for an agency for a zoom level
     */
    async getAvailableTiles(req, res) {
        const agency = req.params.agency;

        const tilesPath = path.join(this.storage, 'tiles', agency, 'tiles.json');

        if (fs.existsSync(tilesPath)) {
            res.set({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Content-Type': 'application/ld+json'
            });
            res.send(await readFile(tilesPath, 'utf8'));
            return;
        } else {
            const versions = await readdir(path.join(this.storage, 'linked_pages', agency));
            const sorted_versions = utils.sortVersions(new Date(), versions);

            // Create tiles for most recent version
            const rootpath = path.join(this.storage, 'linked_pages', agency, sorted_versions[0]);
            const tiles = await this.createTilesList(rootpath, agency).catch(() => {
                res.set({'Cache-Control': 'no-cache'});
                res.status(404).send("No tiles available for " + agency);
            });

            res.set({
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Content-Type': 'application/ld+json'
            });
            res.send(tiles);

            fs.writeFile(tilesPath, JSON.stringify(tiles), 'utf8', ((err) => {
                const logger = Logger.getLogger(utils.serverConfig.logLevel || 'info');
                if (err) {
                    logger.error('Failed to write tiles dataset for ' + agency + ' updated');
                } else {
                    logger.info('Available tiles dataset for ' + agency + ' updated');
                }
            }));
        }
    }

    /**
     * Returns JSON-LD as object containing all existing tiles for the version specified at rootpath
     * @param {*} rootpath: Path including the version
     * @param {*} agency
     */
    async createTilesList(rootpath, agency) {
        let dataset = this.getDataset(agency);
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
            "@id":  host + "/" + agency + '/tiles',
            "@graph": []
        };

        const rootDir = await readdir(rootpath);
        for (let zoom of rootDir) {
            let zoomDir = await readdir(rootpath + '/' + zoom);
            for (let x of zoomDir) {
                let xDir = await readdir(rootpath + '/' + zoom + '/' + x);
                for (let y of xDir) {

                    const key = host + "/" + agency + '/connections/' + zoom + '/' + x + '/' + y;
                    skeleton['@graph'].push({
                        "@id": key,
                        "dct:spatial": dataset['geographicArea'] || "",
                        "tiles:zoom": zoom,
                        "tiles:longitudeTile": x,
                        "tiles:latitudeTile": y,
                    });
                }
            }
        }

        return skeleton;
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