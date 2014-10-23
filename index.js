var parser = require('xml2json');

var fs = require('fs');

var XmlParser = require('./xml_parser.js');
var Writable = require('stream').Writable
var util = require('util');

var mongo = require('mongodb')
var MongoClient = mongo.MongoClient;
var ObjectID = mongo.ObjectID;

function noop(){}

util.inherits(MongoUploader, Writable);

function MongoUploader(source, options){
	if (!(this instanceof MongoUploader))
		return new MongoUploader(source, options);

	Writable.call(this, options);

	var watchDogObj;
	function watchDogCallbask(){
		self._db.close();
	}

	function startWatchDog(){
		var delay = 2000;
		watchDogObj = setTimeout(watchDogCallbask, delay);	
	}

	this._kickWatchDog = function kickWatchDog(){
		clearTimeout(watchDogObj);
		startWatchDog();
	}

	this._source = source;
	this._maxConnections = 20;
	this._currentConnections = 0;
	this._buffer = [];
	this._count = 0;
	this._connectionEstablishedCallback = noop;
	this._db = null;
	this._collection = null;
	var self = this;
	
	MongoClient.connect("mongodb://localhost:27017/turbineLog", function(err, db) {
		if(err) throw err;
		
		self._db = db;
		startWatchDog();
		self._connectionEstablishedCallback();
	});
}



MongoUploader.prototype._write = function _write(chunk, encoding, callback){
	var self = this;
	function insertData(json, callback){
		self._kickWatchDog();
		var jsonObj = JSON.parse(json);
		self._currentConnections++;

		self._collection.insert(jsonObj, function(err, status){
			if(self._currentConnections >= self._maxConnections)
				callback(); // one new spot available
			self._currentConnections--;
			if(err) throw err;
		});

		if(self._currentConnections < self._maxConnections){
			callback(); // one new spot available			
		}

	}

	if (chunk === null)
		return this.push('');

	if(encoding === 'buffer')
		var json = this._buffer + chunk.asciiSlice();
	else
		throw new Error("Buffer not supported: " + encoding)

	if(this._db){
		insertData(json, callback);
	}else{
		this._connectionEstablishedCallback = function(json){
			self._collection = self._db.collection("Turbine3000364Log");
			insertData(json, callback);
		}.bind(this, json);
	}
};

// var files = [
// 	'Turbine3000363Log.xml',
// 	'Turbine3000364Log.xml',
// 	'Turbine3000365Log.xml',
// 	'Turbine3000366Log.xml'
// 	];
fs.createReadStream(__dirname + '/FourTurbineSite/Turbine3000364Log.xml', { 
	flags: 'r',
	encoding: 'utf8',
	highWaterMark: 4096,
	autoClose: true
})
	.pipe(XmlParser({encoding: 'utf8'}))
	.pipe(MongoUploader());












