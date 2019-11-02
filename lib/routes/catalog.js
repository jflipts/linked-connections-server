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
                && (await readdir(this.storage + '/linked_pages/' + dataset.companyName)).length > 0) {

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
        let unsorted = (await readdir(lp_path)).map(v => {
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
            let startDateFile = (await readdir(lp_path + '/' + sorted[sorted.length - 1].toISOString()))[0];
            let startDate = startDateFile.substring(0, startDateFile.indexOf('.jsonld.gz'));

            let endDateFolder = await readdir(lp_path + '/' + sorted[0].toISOString());
            let endDateFile = endDateFolder[endDateFolder.length - 1];
            // TODO, enddate can be in any of the subfolders. This code is also broken if not in subfolders
            let endDateData
            if (fs.lstatSync(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile).isDirectory()) {
                let endDateFolder2 = await readdir(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile);
                let endDateFile2 = endDateFolder2[endDateFolder2.length - 1];
                let endDateFolder3 = await readdir(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile + '/' + endDateFile2);
                let endDateFile3 = endDateFolder3[endDateFolder3.length - 1];
                endDateData = (await this.utils.readAndGunzip(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile + '/' + endDateFile2 + '/' + endDateFile3)).split(',\n');
            } else {
                endDateData = (await this.utils.readAndGunzip(lp_path + '/' + sorted[0].toISOString() + '/' + endDateFile)).split(',\n');
            }

            let endDate = JSON.parse(endDateData[endDateData.length - 1])['departureTime'];
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