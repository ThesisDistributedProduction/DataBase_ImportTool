var Transform = require('stream').Transform
var util = require('util');
var parser = require('xml2json');

util.inherits(XmlParser, Transform);

function XmlParser(source, options){
	if (!(this instanceof XmlParser))
		return new XmlParser(source, options);

	Transform.call(this, options);

	this._source = source;
	this._firstChunk = true;
	this._buffer = '';
	this._count = 0;
	var self = this;

}

XmlParser.prototype._transform = function _transform(chunk, encoding, callback){

	if(encoding === 'buffer')
		var xml = this._buffer + chunk.asciiSlice();
	else
		throw new Error("Buffer not supported: " + encoding)

	// if the source doesn't have data, we don't have data yet.
	if (chunk === null)
		return this.push('');

	if(this._firstChunk){
		xml = xml.replace('<TurbineLog>', '');
		this._firstChunk = false;
	}
	
	var parts = xml.split('>')
	
	var i = 0
	for(; i < parts.length - 1; i++){
		try{
		var xmlNode = parts[i] + '>';
		var json = parser.toJson(xmlNode);
		var objStart = json.indexOf('{', 2);
		}catch(e){
			if(xmlNode == '</TurbineLog>')
				return true;
			else{
				console.log(e);
				console.log(json);
				console.log(xmlNode);
				continue;
			}
		}
		json = json.slice(objStart, -1)
		
		this.push(json);
		if(++this._count % 5000 === 0)
			console.log(this._count);
	}
	
	if(i === 0)
		this.push('');
	this._buffer = parts[i];

	callback();
	return true;
}

module.exports = XmlParser;


