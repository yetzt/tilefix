#!/usr/bin/env node

// tilefix -z 5-12 -b left,bottom,right,top file.mbtiles action.js

const zlib = require("zlib");

const debug = require("debug")("tilefix");
const mbtiles = require("@mapbox/mbtiles");
const queue = require("queue");

const vt = require("@mapbox/vector-tile");
const gvt = require("@mapgis/geojson-vt");
const pbf = require("pbf");
const vtpbf = require("vt-pbf");

function tilefix(opts, fn){
	if (!(this instanceof tilefix)) return new tilefix(opts, fn);

	var self = this;
	self.opts = opts;
	
	self.queue = queue({ concurrency: 1 });
	self.mbtiles = null;

	new mbtiles(self.opts.src+'?mode=rw', function(err, m){
		if (err) return fn(err);
		self.mbtiles = m;
		self.mbtiles.getInfo(function(err,info){
			if (err) return fn(err);
			
			debug("tileset: %s (%s)", info.id, info.name);
			
			// check if format is pbf
			if (info.format !== "pbf") return fn(new Error("mbtiles does not contain pbf"));
			
			// retain scheme
			self.opts.scheme = info.scheme.split("");
			
			// check zoom levels
			var zoom = [
				Math.max(info.minzoom, Math.min(info.maxzoom, self.opts.zoom[0])),
				Math.max(info.minzoom, Math.min(info.maxzoom, self.opts.zoom[1])),
			];
			if (zoom.join("-") !== self.opts.zoom.join("-")) {
				debug("adjsuting zoom: [ %s ] → [ %s ]", self.opts.zoom.join(", "), zoom.join(", "));
				self.opts.zoom = zoom;
			}

			// expand zoom range into array of levels
			self.opts.zoom = self.range(self.opts.zoom);

			// check bounds
			var bbox = [
				Math.max(info.bounds[0], Math.min(info.bounds[2], self.opts.bbox[0])),
				Math.max(info.bounds[1], Math.min(info.bounds[3], self.opts.bbox[1])),
				Math.max(info.bounds[0], Math.min(info.bounds[2], self.opts.bbox[2])),
				Math.max(info.bounds[1], Math.min(info.bounds[3], self.opts.bbox[3])),
			];
			if (bbox.join(",") !== self.opts.bbox.join(",")) {
				debug("adjusting bbox: [ %s ] → [ %s ]", self.opts.bbox.join(", "), bbox.join(", "));
				self.opts.bbox = bbox;
			}
			
			// calculate number of tiles to process and pepare queue
			var numTiles = 0;
			var tileQueue = [];
			self.opts.zoom.forEach(function(z){
				var qdef = [z,
					self.range([
						self.lonTile(bbox[0], z),
						self.lonTile(bbox[2], z)
					]),
					self.range([
						self.latTile(bbox[1], z),
						self.latTile(bbox[3], z)
					])
				];
				numTiles += (qdef[1].length*qdef[2].length);
				tileQueue.push(qdef);
			});
			debug("tiles to process: %d", numTiles);
			
			// wait on writable
			self.mbtiles.startWriting(function(err) {
				if (err) return fn(err);
			
				// process all tiles within bounding box
				tileQueue.forEach(function(def){
					var z = def[0];
					def[1].forEach(function(x){
						def[2].forEach(function(y){
							self.queue.push(function(next){
								self.process(x,y,z,next);
							});
						});
					});
				});
				
				self.queue.start(function(err){
					if (err) return fn(err);
					
					// save and close mbtiles
					debug("writing mbtiles"); 
					self.mbtiles.stopWriting(function(err) {
						if (err) return fn(err);
						fn(null);
					});
				});
			});
		});
	});
};

tilefix.prototype.process = function(x,y,z,fn) {
	var self = this;
	var id = [z,x,y].join("/");
	debug("[processing] %s", id);

	self.mbtiles.getTile(z, x, y, function(err, data, headers){
		if (err) return fn(err);
		
		zlib.gunzip(data, function(err, buffer){
			if (err) return fn(err);

			var tile = new vt.VectorTile(new pbf(buffer));

			// convert to geojson
			var collections = {};
			Object.keys(tile.layers).forEach(function(l) {
				collections[l] = { type: 'FeatureCollection', features: [] };
				for (var i = 0; i < tile.layers[l].length; i++) {
					collections[l].features.push(tile.layers[l].feature(i).toGeoJSON(x, y, z));
				}
			});
			
			// hand features to user-provided script
			self.opts.fn(collections, function(err,collections){
				if (err) return fn(err);
				if (collections === null) return debug("[no change] %s", id), fn(null);

				var result = {};
				Object.keys(collections).forEach(function(l){
					result[l] = gvt(collections[l], {
						tolerance: 0, // do not douglas-peucker this stuff
						extent: 4096, // FIXME: take from info
						buffer: 4096, // stupid fact
						indexMaxZoom: z,
					}).getTile(z, x, y);
				});
			
				zlib.gzip(vtpbf.fromGeojsonVt(result), { level: 4 }, function(err, buffer){
					if (err) return fn(err);

					// FIXME: save buffer
					self.mbtiles.putTile(z, x, y, buffer, function(err){
						if (err) return fn(err);
						debug("[saved] %s", id)
						fn(null);
					});
				});
			});
		});
	});

	return this;
};

tilefix.prototype.lonTile = function(lon,zoom) {
	return (Math.floor((lon+180)/360*Math.pow(2,zoom)));
};

tilefix.prototype.latTile = function(lat,zoom) {
	return (Math.floor((1-Math.log(Math.tan(lat*Math.PI/180) + 1/Math.cos(lat*Math.PI/180))/Math.PI)/2 *Math.pow(2,zoom)));
};

tilefix.prototype.range = function(r){
	var z = []; 
	r.sort(function(a,b){ return a-b; }); 
	for (var i = r[0]; i <= r[1]; i++) z.push(i); 
	return z;
};

if (require.main === module) {

	const argv = require("minimist")(process.argv.slice(2).join(" ").replace(/-b(box)?\s+-/,'-b$1=-').split(" "), { string: [ "z", "b" ],alias: { z: "zoom", b: "bbox", t: "tiles", s: "script", h: "help", v: "verbose" }});

	const path = require("path");
	const fs = require("fs");

	var opts = {};

	if (!!opts.h) {
		console.error("Usage: %s [-z 0-12] [-b -180,-90,189,90] -t tiles.mbtiles -s script.js", path.basename(process.argv[1]));
		console.error(" -z, --zoom      zoom levels");
		console.error(" -b, --bbox      bounding box (must use equal sign!)");
		console.error(" -t, --tiles     mbtiles file");
		console.error(" -s, --script    script file");
		console.error(" -h, --help      show usage");
		console.error(" -v, --verbose   show debug output");
		console.error("");
		process.exit(0);
	}
	
	// verbosity (in reality just a convenient way for DEBUG=tilefix)
	if (!!argv.v) debug.enabled = true;
	
	// check for mbtiles and js file in args
	if (!argv.t) argv.t = argv._.find(function(v){ return /\.mbtiles/.test(v); });
	if (!argv.s) argv.s = argv._.find(function(v){ return /\.js/.test(v); });
	
	if (!argv.t) console.error("Error: no mbtiles file specified"), process.exit(1);
	if (!argv.s) console.error("Error: no script specified"), process.exit(1);
	
	// resolve paths
	argv.t = path.resolve(process.cwd(), argv.t);
	argv.s = path.resolve(process.cwd(), argv.s);
	
	// check if files exist
	if (!fs.existsSync(argv.t)) console.error("Error: mbtiles file not found"), process.exit(1);
	if (!fs.existsSync(argv.s)) console.error("Error: script file not found"), process.exit(1);
	
	// sanitize zoom levels
	if (!argv.z) argv.z = "";
	argv.z = argv.z.split(/[^0-9]+/g).map(function(v){ return parseInt(v,10) }).filter(function(v){ return !isNaN(v) });
	switch (argv.z.length) {
		case 0: argv.z = [0, 24]; break; // all possible zoom levels
		case 1: argv.z = [ argv.z[0], argv.z[0] ]; break;
		default: argv.z = argv.z.sort(function(a,b){ return a-b; }); argv.z = argv.z.slice(0,1).concat(argv.z.slice(-1));
	}
	argv.z = argv.z.map(function(v){ return Math.max(0,Math.min(v,24)) }); // cap 0-24
	
	// sanitize bounding box
	if (!argv.b) argv.b = "-180,-90,180,90";
	argv.b = argv.b.split(/[^0-9\.\-\+]+/g);
	if (argv.b.length !== 4) console.error("Error: invalid bounding box"), process.exit(1);
	argv.b[0] = Math.min(180,Math.max(-180,argv.b[0]));
	argv.b[2] = Math.min(180,Math.max(-180,argv.b[2]));
	argv.b[1] = Math.min(90,Math.max(-90,argv.b[1]));
	argv.b[3] = Math.min(90,Math.max(-90,argv.b[3]));

	// call 
	tilefix({
		src: argv.t,
		fn: require(argv.s),
		zoom: argv.z,
		bbox: argv.b
	}, function(err){
		if (err) return console.error("Error: %s", err), process.exit(1);
		process.exit(0); //yay
	});
	
} else {
	module.exports = tilefix;
}