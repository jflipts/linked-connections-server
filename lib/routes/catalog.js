const util = require('util');
const fs = require('fs');

const readdir = util.promisify(fs.readdir);
const writeFile = util.promisify(fs.writeFile);

class Catalog {
    constructor() {
        this._utils = require('../utils/utils');
        this._storage = this.utils.datasetsConfig.storage;
        this._datasets = this.utils.datasetsConfig.datasets;
        this._server_config = this.utils.serverConfig;
    }

    async getCatalog(req, res) {
        res.set({
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Content-Type': 'application/ld+json'
        });

        if (!fs.existsSync(this.storage + '/datasets/catalog.json')) {
            let catalog = await this.createCatalog();
            this.saveCatalog(catalog);
            res.json(catalog);
        } else {
            res.json(JSON.parse(fs.readFileSync(this.storage + '/datasets/catalog.json')));
        }
    }

    saveCatalog(catalog) {
        writeFile(this.storage + '/datasets/catalog.json', JSON.stringify(catalog), 'utf8');
    }

    async createCatalog() {
        let catalog = {
            "@context": {
                "xsd": "http://www.w3.org/2001/XMLSchema#",
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "owl": "http://www.w3.org/2002/07/owl#",
                "schema": "http://schema.org/",
                "access": "http://publications.europa.eu/resource/authority/access-right/",
                "dct:modified": {
                    "@type": "xsd:dateTime"
                },
                "dct:rights": {
                    "@type": "@id"
                },
                "dct:accessRights": {
                    "@type": "@id"
                },
                "dct:issued": {
                    "@type": "xsd:dateTime"
                },
                "dct:spatial": {
                    "@type": "@id"
                },
                "dct:license": {
                    "@type": "@id"
                },
                "dct:conformsTo": {
                    "@type": "@id"
                },
                "dcat:mediaType": {
                    "@container": "@set"
                },
                "schema:startDate": {
                    "@type": "xsd:dateTime"
                },
                "schema:endDate": {
                    "@type": "xsd:dateTime"
                }
            },
            "@id": (this.server_config.protocol || "http") + "://" + this.server_config.hostname + "/catalog",
            "@type": "dcat:Catalog",
            "dct:title": "Catalog of Linked Connection datasets",
            "dct:description": "Catalog of Linked Connection datasets published by " + this.utils.datasetsConfig.organization.name,
            "dct:modified": new Date().toISOString(),
            "dct:license": "http://creativecommons.org/publicdomain/zero/1.0/",
            "dct:rights": "access:PUBLIC",
            "dct:publisher": {
                "@id": this.utils.datasetsConfig.organization.id,
                "@type": "foaf:Organization",
                "foaf:name": this.utils.datasetsConfig.organization.name
            },
            "dcat:dataset": []
        };

        await Promise.all(this.datasets.map(async dataset => {
            // Check there is existing data about this dataset
            if (fs.existsSync(this.storage + '/linked_pages/' + dataset.companyName)
                && (fs.readdirSync(this.storage + '/linked_pages/' + dataset.companyName)).length > 0) {

                let dcatDataset = {
                    "@id": (this.server_config.protocol || "http") + "://" + this.server_config.hostname + "/" + dataset.companyName + "/Connections",
                    "@type": "dcat:Dataset",
                    "dct:description": "Linked Connections dataset for " + dataset.companyName,
                    "dct:title": dataset.companyName + " Linked Connections",
                    "dct:spatial": dataset.geographicArea || "",
                    "dcat:keyword": dataset.keywords,
                    "dct:conformsTo": "http://linkedconnections.org/specification/1-0",
                    "dct:accessRights": "access:PUBLIC",
                    "dcat:distribution": [await this.getDistribution(dataset, 'connections')]
                };

                let stopsDataset = {
                    "@id": (this.server_config.protocol || "http") + "://" + this.server_config.hostname + "/" + dataset.companyName + "/Stops",
                    "@type": "dcat:Dataset",
                    "dct:description": "Stops dataset for " + dataset.companyName,
                    "dct:title": dataset.companyName + " stops",
                    "dct:spatial": dataset.geographicArea || "",
                    "dcat:keyword": ['Stops', 'Stations'],
                    "dct:accessRights": "access:PUBLIC",
                    "dcat:distribution": [await this.getDistribution(dataset, 'stops')]
                };

                catalog['dcat:dataset'].push(dcatDataset);
                catalog['dcat:dataset'].push(stopsDataset);
            }
        }));

        return catalog;
    }

    async getDistribution(dataset, type) {
        let lp_path = this.storage + '/linked_pages/' + dataset.companyName;
        let unsorted = (fs.readdirSync(lp_path)).map(v => {
            return new Date(v);
        });
        let sorted = this.utils.sortVersions(new Date(), unsorted);

        // TODO Access url will change for tiled configs
        let dist = {
            "@id": (this.server_config.protocol || "http") + "://" + this.server_config.hostname + "/" + dataset.companyName + "/" + type,
            "@type": "dcat:Distribution",
            "dcat:accessURL": (this.server_config.protocol || "http") + "://" + this.server_config.hostname + "/" + dataset.companyName + "/" + type,
            "dct:spatial": (dataset.geographicArea || ""),
            "dct:license": "http://creativecommons.org/publicdomain/zero/1.0/",
            "dcat:mediaType": ['application/ld+json', 'text/turtle', 'application/trig', 'application/n-triples'],
            "dct:issued": sorted[sorted.length - 1].toISOString(),
            "dct:modified": sorted[0].toISOString()
        };

        if (type === 'connections') {
            let startDate = Infinity;
            let endDate = new Date(0)

            if (dataset.tilingStrategy == "onelevel") {
                // Oldest data is in oldest version
                const startDateFilePath = lp_path + '/' + sorted[sorted.length - 1].toISOString() + '/' + dataset.tilingOptions.zoom;

                // Loop over all folders of folders to find the start date
                let zoomDir = fs.readdirSync(startDateFilePath);
                for (const latDirName of zoomDir) {
                    let latDir = fs.readdirSync(startDateFilePath + '/' + latDirName);
                    for (const lonDirName of latDir) {
                        let lonDir = fs.readdirSync(startDateFilePath + '/' + latDirName + '/' + lonDirName)

                        const lowestDateFile = lonDir[0];
                        const lowestDate = new Date(lowestDateFile.substring(0, lowestDateFile.indexOf('.jsonld.gz')));
                        if (lowestDate < startDate) {
                            startDate = lowestDate;
                        }
                    }
                }

                // Latest date is in newest version
                const endDateFilePath = lp_path + '/' + sorted[0].toISOString() + '/' + dataset.tilingOptions.zoom;

                // Loop over all folders of folders to find the end date
                zoomDir = fs.readdirSync(endDateFilePath);
                for (const latDirName of zoomDir) {
                    let latDir = fs.readdirSync(endDateFilePath + '/' + latDirName);
                    for (const lonDirName of latDir) {
                        let lonDir = fs.readdirSync(endDateFilePath + '/' + latDirName + '/' + lonDirName)

                        const highestDateFile = lonDir[lonDir.length - 1];
                        // TODO: Not actually last connection
                        const highestDate = new Date(highestDateFile.substring(0, highestDateFile.indexOf('.jsonld.gz')));

                        // const highestDateData = (await this.utils.readAndGunzip(endDateFilePath + '/' + latDirName + '/' + lonDirName + '/' + highestDateFile)).split(',\n');
                        // const highestDate = new Date(JSON.parse(highestDateData[highestDateData.length - 1])['departureTime']);
                        if (highestDate > endDate) {
                            endDate = highestDate;
                        }
                    }
                }

            } else {
                let startDateFile = (fs.readdirSync(lp_path + '/' + sorted[sorted.length - 1].toISOString()))[0];
                startDate = startDateFile.substring(0, startDateFile.indexOf('.jsonld.gz'));

                let endDateFolder = fs.readdirSync(lp_path + '/' + sorted[0].toISOString());
                let endDateFile = endDateFolder[endDateFolder.length - 1];
                let endDateData = (await this.utils.readAndGunzip(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile)).split(',\n');
                endDate = JSON.parse(endDateData[endDateData.length - 1])['departureTime'];
            }

            dist['schema:startDate'] = startDate;
            dist['schema:endDate'] = endDate;
        }

        return dist;
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

module.exports = Catalog;