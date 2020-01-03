const express = require('express');
const router = express.Router();
const PageFinder = require('./page-finder.js');
const Memento = require('./memento.js');
const Catalog = require('./catalog.js');
const Stops = require('./stops.js');
const Tiles = require('./tiles')

router.get('/:agency/connections', (req, res) => {
    new PageFinder().getConnections(req, res);
});

router.get('/:agency/connections/:zoom/:x/:y', (req, res) => {
    new PageFinder().getConnectionsTile(req, res);
});

router.get('/:agency/connections/:longitude/:latitude', (req, res) => {

})

router.get('/:agency/connections/memento', (req, res) => {
    new Memento().getMemento(req, res);
});

router.get('/catalog', (req, res) => {
    new Catalog().getCatalog(req, res);
});

router.get('/:agency/stops', (req, res) => {
    new Stops().getStops(req, res);
});

router.get('/:agency/tiles', (req, res) => {
    new Tiles().getAvailableTiles(req, res);
});

router.options('/*', (req, res) => {
    res.set({
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': '*',
        'Content-Type': 'application/ld+json'
    });

    res.send(200);
});

module.exports = router;