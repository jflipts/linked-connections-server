const utils = require('../utils/utils');
const tileUtils = require('../utils/tileUtils');
const fs = require('fs');
const path = require('path');


const relationSkeleton =
    `{
    "@type": "tree:GeospatiallyContainsRelation",
    "tree:node": {
        "@id": ""
    },
    "tree:path": ["lc:departureStop", "geosparql:asWKT"],
    "tree:value": {
        "@type": "tree:Node",
        "geosparql:asWKT": ""
    }
}`;

const leafNodeSkeletion =
    `{
    "@type": "tree:Node",
    "tree:view": ""
}`;

module.exports = new class TreeUtils {

    async addTreeOntologyToPage(params, metadata) {
        const { host, agency } = params;

        const skeleton = JSON.parse(leafNodeSkeletion);
        skeleton["tree:view"] = host + agency + '/connections/';

        return { ...metadata, ...skeleton };
    }

    generateTreeOntologyInternalNode(tile, children) {

        const template = fs.readFileSync('./statics/skeleton_tiles_internal.jsonld', { encoding: 'utf8' });
        let skeleton = JSON.parse(template);

        skeleton["@id"] = path.join("connections", String(tile.zoom), String(tile.x), String(tile.y));
        skeleton["tiles:zoom"] = tile.zoom;
        skeleton["tiles:longitudeTile"] = tile.x;
        skeleton["tiles:latitudeTile"] = tile.y;

        children.forEach((child) => {
            const relation = JSON.parse(relationSkeleton);
            relation["tree:node"]["@id"] = path.join("connections", String(child.zoom), String(child.x), String(child.y));

            let boundingBox = tileUtils.getBBox(child.x, child.y, child.zoom);
            boundingBox = boundingBox.map((corner) => {
                return String(corner.longitude) + " " + String(corner.latitude)
            })
            relation["tree:value"]["geosparql:asWKT"] = "POLYGON((" + boundingBox.join(', ') + "))";

            skeleton["tree:relation"].push(relation);
        });

        return skeleton;
    }

    async buildInternalTree(rootPath, options, logger) {
        const { minZoom } = options.tilingOptions;

        let zoomLevels = fs.readdirSync(rootPath).map((a) => parseInt(a));

        // Index zoom is level of children
        for (let zoom = Math.max(...zoomLevels); zoom > 0; zoom--) {
            const parentMap = tileUtils.parentMap(rootPath, zoom);

            for ([parent, tiles] of parentMap) {
                const [pzoom, x, y] = parent.split(",");
                const parentTile = {
                    "x": parseInt(x),
                    "y": parseInt(y),
                    "zoom": parseInt(pzoom),
                    "numberOfChildren": tiles.length
                }

                const parentPath = path.join(rootPath, pzoom, x, y);
                fs.mkdirSync(parentPath, { recursive: true });

                const internalNode = this.generateTreeOntologyInternalNode(parentTile, tiles);
                fs.writeFileSync(path.join(parentPath, "internal.jsonld"), JSON.stringify(internalNode));
            }

            if (parentMap.size == 1 && zoom <= Math.min(...zoomLevels)) {
                return
            }
        }
    }

    async completeInternalNode(params) {
        const { storage, agency, version, zoom, x, y, host } = params;

        const pathToNode = path.join(storage, "linked_pages", agency, version, zoom, x, y, "internal.jsonld.gz");
        const node = await utils.readAndGunzip(pathToNode);
        const json = JSON.parse(node);

        const basePath = path.join(host, agency);
        json["@id"] = path.join(basePath, json["@id"]);
        json["tree:view"] = path.join(basePath, "/connections");

        json["tree:relation"].forEach((relation) => {
            relation["tree:node"]["@id"] = path.join(basePath, relation["tree:node"]["@id"]);
        });

        json['hydra:search']['hydra:template'] = host + agency + '/connections/{zoom}/{x}/{y}{?departureTime}';

        return json
    }

    getRootNode(storage, agency) {
        const versions = fs.readdirSync(path.join(storage, 'linked_pages', agency));
        const sorted_versions = utils.sortVersions(new Date(), versions);

        // Calculate root of tree for most recent version
        const rootpath = path.join(storage, 'linked_pages', agency, sorted_versions[0]);

        const rootDir = fs.readdirSync(rootpath);
        const zoom = Math.min(...rootDir);
        const zoomDir = fs.readdirSync(rootpath + '/' + zoom);
        const x = Math.min(...zoomDir);
        const xDir = fs.readdirSync(rootpath + '/' + zoom + '/' + x);
        const y = Math.min(...xDir);
        return {zoom, x, y}
    }
}