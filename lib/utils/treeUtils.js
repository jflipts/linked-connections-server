const utils = require('../utils/utils');
const tileUtils = require('../utils/tileUtils');
const fs = require('fs');
const path = require('path');


const internalNodeSkeleton =
`{
    "@type": "tree:Node",
    "tree:relation": [],
    "dcterms:isPartOf": ""
}`;

const relationSkeleton =
`{
    "@type": "jeroen:GeospatialTileRelation ",
    "tree:node": {
        "@id": ""
    },
    "shacl:path": "lc:departureStop",
    "tree:value": {
        "@type": "tree:Node",
        "tiles:zoom": "",
        "tiles:longitudeTile": "",
        "tiles:latitudeTile": ""
    }
}`;

const leafNodeSkeletion =
`{
    "@type": "tree:Node",
    "dcterms:isPartOf": ""
}`;

module.exports = new class TreeUtils {

    async addTreeOntologyToPage(params, metadata) {
        const {host, agency} = params;

        const skeleton = JSON.parse(leafNodeSkeletion);
        skeleton["dcterms:isPartOf"] = host + agency + '/connections/';

        return {...metadata, ...skeleton};
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
            relation["tree:value"]["tiles:zoom"] = child.zoom;
            relation["tree:value"]["tiles:longitudeTile"] = child.x;
            relation["tree:value"]["tiles:latitudeTile"] = child.y;

            skeleton["tree:relation"].push(relation);
        });

        return skeleton;
    }

    async buildInternalTree(rootPath, options, logger) {
        const {minZoom} = options.tilingOptions;

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

            if(parentMap.size == 1 && zoom <= Math.min(...zoomLevels)) {
                return
            }
        }
    }

    async completeInternalNode(params) {
        const {storage, agency, version, zoom, x, y, host} = params;

        const pathToNode = path.join(storage, "linked_pages", agency, version, zoom, x, y, "internal.jsonld.gz");
        const node = await utils.readAndGunzip(pathToNode);
        const json = JSON.parse(node);

        const basePath = path.join(host, agency);
        json["@id"] = path.join(basePath, json["@id"]);
        json["dcterms:isPartOf"] = path.join(basePath, "/connections");

        json["tree:relation"].forEach((relation) => {
            relation["tree:node"]["@id"] = path.join(basePath, relation["tree:node"]["@id"]);
        });

        json['@context']['hydra:search']['hydra:template'] = host + agency + '/connections/{zoom}/{x}/{y}{?departureTime}';

        return json
    }

}