const express = require('express');
const router = express.Router();
const fs = require('fs');
const zlib = require('zlib');

const config = JSON.parse(fs.readFileSync('./datasets_config.json', 'utf8'));
let storage = config.storage;

router.get('/:agency/:version/:resource', function (req, res) {
    let agency = req.params.agency;
    let version = req.params.version;
    let resource = req.params.resource;
    let buffer = [];

    if(storage.endsWith('/')) {
        storage = storage.substring(0, storage.length - 1);
    }

    fs.createReadStream(storage + '/linked_pages/' + agency + '/' + version + '/' + resource + '.jsonld.gz')
        .pipe(new zlib.createGunzip())
        .on('data', function (data) {
            buffer.push(data);
        })
        .on('end', function () {
            var jsonld_graph = buffer.join('').split(',\n');
            fs.readFile('./statics/skeleton.jsonld', { encoding: 'utf8' }, (err, data) => {
                var jsonld_skeleton = JSON.parse(data);
                jsonld_skeleton['@id'] = jsonld_skeleton['@id'] + 'memento/' + agency + '/' + version + '/' + resource;
                jsonld_skeleton['hydra:next'] = jsonld_skeleton['hydra:next'] + 'memento/'
                    + agency + '/' + version + '/' + getAdjacentPage(agency + '/' + version, resource, true);
                jsonld_skeleton['hydra:previous'] = jsonld_skeleton['hydra:previous'] + 'memento/'
                    + agency + '/' + version + '/' + getAdjacentPage(agency + '/' + version, resource, false);
                jsonld_skeleton['hydra:search']['hydra:template'] = jsonld_skeleton['hydra:search']['hydra:template'] + 'memento/' + agency + '/' + version + '/{?departureTime}';

                for (let i in jsonld_graph) {
                    jsonld_skeleton['@graph'].push(JSON.parse(jsonld_graph[i]));
                }

                res.json(jsonld_skeleton);
            });
        });
});


function getAdjacentPage(path, departureTime, next) {
    var date = new Date(departureTime);
    if (next) {
        date.setMinutes(date.getMinutes() + 10);
    } else {
        date.setMinutes(date.getMinutes() - 10);
    }
    while (!fs.existsSync(storage + '/linked_pages/' + path + '/' + date.toISOString() + '.jsonld.gz')) {
        if (next) {
            date.setMinutes(date.getMinutes() + 10);
        } else {
            date.setMinutes(date.getMinutes() - 10);
        }
    }

    return date.toISOString();
}

module.exports = router;