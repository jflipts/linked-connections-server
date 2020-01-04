const utils = require('../utils/utils');
const fs = require('fs');
const pageWriterStream = require('../manager/pageWriterStream');
const jsonldstream = require('jsonld-stream');


module.exports = new class TileUtils {
    long2tile(lon, zoom) {
        return (Math.floor((lon + 180) / 360 * Math.pow(2, zoom)));
    }
    lat2tile(lat, zoom) {
        return (Math.floor((1 - Math.log(Math.tan(lat * Math.PI / 180) + 1 / Math.cos(lat * Math.PI / 180)) / Math.PI) / 2 * Math.pow(2, zoom)));
    }
    tile2long(x, z) {
        return (x / Math.pow(2, z) * 360 - 180);
    }
    tile2lat(y, z) {
        var n = Math.PI - 2 * Math.PI * y / Math.pow(2, z);
        return (180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))));
    }

    getBBox(x, y, zoom) {
        const lat1 = this.tile2lat(y, zoom);
        const lat2 = this.tile2lat(y + 1, zoom);
        const lon1 = this.tile2long(x, zoom);
        const lon2 = this.tile2long(x + 1, zoom);

        return [
            { longitude: lon1, latitude: lat1 }, // TL
            { longitude: lon2, latitude: lat1 }, // TR
            { longitude: lon2, latitude: lat2 }, // BR
            { longitude: lon1, latitude: lat2 }, // BL
        ];
    }

    /**
     * Calculate the parent of tile
     *
     * @param {*} z
     * @param {*} x
     * @param {*} y
     *
     * @returns [pz, px, py]
     */
    parent(z, x, y) {
        const lon = this.tile2long(x, z);
        const lat = this.tile2lat(y, z);

        const xParent = this.long2tile(lon, z - 1);
        const yParent = this.lat2tile(lat, z - 1);

        return [z - 1, xParent, yParent];
    }

    /**
     * Hashes all the tiles of a zoomlevel to its parent tile
     *
     * @param {*} rootpath: version directory
     * @param {*} zoom: zoomlevel of tiles
     *
     * @returns Map(<parentTile>,[<childrenTile>])
     */
    parentMap(rootpath, zoom) {
        const parentMap = new Map();

        // Loop over all folders of folders
        // Hash every tile to its parent tile
        let zoomDir = fs.readdirSync(rootpath + '/' + zoom);
        for (let x of zoomDir) {
            let xDir = fs.readdirSync(rootpath + '/' + zoom + '/' + x);
            for (let y of xDir) {
                let totalSize = 0;

                let yDir = fs.readdirSync(rootpath + '/' + zoom + '/' + x + '/' + y);
                for (const page of yDir) {
                    const pagePath = rootpath + '/' + zoom + '/' + x + '/' + y + '/' + page;
                    totalSize += fs.statSync(pagePath).size
                }

                const tile = {
                    "x": parseInt(x),
                    "y": parseInt(y),
                    "zoom": zoom,
                    "totalSize": totalSize
                }

                const parent = this.parent(tile.zoom, tile.x, tile.y);

                const key = parent.join(",");
                const value = parentMap.get(key) || [];
                value.push(tile);
                parentMap.set(key, value);
            }
        }

        return parentMap;
    }

    /**
     * Combines iteratively merges 4 tiles until no new tile can be formed that
     * is smaller than the threshold or the max zoom level is reached.
     * Cleans up empty directorties that would be created.
     *
     * @param {*} rootpath: version directory
     * @param {*} options: Object should contain tilingOptions: zoom, joinThreshold and minZoom
     * @param {*} logger: Each level will print some stats
     */
    async multilevelTiler(rootpath, options, logger) {
        const { zoom, joinThreshold, minZoom } = options.tilingOptions;

        const parentMap = this.parentMap(rootpath, zoom);

        // Tiles that are pruned at a deeper level
        let illegalTiles = new Map();

        // Bottom up combining of tiles
        let nextZoom = zoom - 1;
        while (nextZoom >= minZoom && parentMap.size > 0) {
            logger.info('Multilevel tiling ' + options.companyName + ' Level ' + nextZoom + '...');
            const joinedMap = await tileCombiner(rootpath, joinThreshold, parentMap, options, illegalTiles);

            // Next round has new parents
            parentMap.clear();

            // Compute illegal tiles for next level
            const newIllegalTiles = new Map();
            illegalTiles.forEach((val, key) => {
                const parent = this.parent(...val);
                newIllegalTiles.set(parent.join(","), parent);
            });
            illegalTiles = newIllegalTiles;

            joinedMap.forEach(tile => {
                const parent = this.parent(tile.zoom, tile.x, tile.y);

                const key = parent.join(",");
                if (!illegalTiles.has(key)) {
                    const value = parentMap.get(key) || [];
                    value.push(tile);
                    parentMap.set(key, value);
                }
            });

            --nextZoom;
        }

        utils.removeEmptyDirectories(rootpath);
    }
}

async function tileCombiner(rootpath, threshold, parentMap, options, illegalTiles) {
    const joinedParents = [];

    for ([parent, tiles] of parentMap) {
        let parentSize = 0;
        tiles.forEach(tile => {
            parentSize += tile.totalSize;
        });

        const [pzoom, x, y] = parent.split(",");

        const parentTile = {
            "x": parseInt(x),
            "y": parseInt(y),
            "zoom": parseInt(pzoom),
            "totalSize": parentSize
        }

        if (parentSize < threshold) {
            joinedParents.push(parentTile);

            // Write combined tiles
            const parentPath = rootpath + "/" + parentTile.zoom + "/" + parentTile.x + "/" + parentTile.y;
            const childrenPaths = [];
            tiles.forEach(tile => {
                childrenPaths.push(rootpath + "/" + tile.zoom + "/" + tile.x + "/" + tile.y);
            });
            await writeCombinedFile(parentPath, childrenPaths, options);
        } else {
            // If not joined, all higher level tiles become illegal
            const parent = [parentTile.zoom, parentTile.x, parentTile.y];
            illegalTiles.set(parent.join(','), parent);
        }
    }

    return joinedParents;
}

async function writeCombinedFile(parentPath, childrenPaths, options) {
    // Combining sorted streams is difficult. Converting arrays to streams is also not optimal.
    // Instead combine the data in a sorted array and then write to a temporary file.
    // Read this file as a stream and paginate.

    return new Promise((resolve, reject) => {
        const data = []
        childrenPaths.forEach(child => {
            fs.readdirSync(child).forEach(file => {
                const buffer = fs.readFileSync(child + "/" + file);
                data.push(JSON.parse("[" + buffer.toString() + "]"));
            });
        });

        const sorted = data.flat().sort((a, b) => {
            return new Date(Date.parse(a.departureTime)) - new Date(Date.parse(b.departureTime));
        })

        const writedata = sorted.map((con) => JSON.stringify(con));

        // Write file
        fs.mkdirSync(parentPath, { recursive: true });
        const filePath = parentPath + "/" + sorted[0].departureTime + "_tmp" + ".jsonld";
        fs.writeFileSync(filePath, writedata.join('\n'));

        // Read stream and paginate
        const reader = fs.createReadStream(filePath, 'utf8')
            .pipe(new jsonldstream.Deserializer());
        const writer = reader.pipe(new pageWriterStream(parentPath, options.fragmentSize || 50000, options.maxPageLength || Infinity));

        writer.on('error', err => {
            reject(err);
        });
        writer.on('finish', finish => {
            fs.unlinkSync(filePath);
            childrenPaths.forEach(child => {
                fs.rmdirSync(child, { recursive: true });
            })
            resolve(finish);
        });
    });
}