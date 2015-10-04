/*run producer worker
* sudo node --harmony-generators exchangeRateSpider.js producer
* run consumer worker
* sudo node --harmony-generators exchangeRateSpider.js consumer
*/

var Promise = require("bluebird"),
mongo = require('mongoskin'),
co = require('co'),
bs = require('nodestalker'),
cheerio = require('cheerio'),
moment = require('moment'),
rp = require('request-promise');

var success_count = 0, 
    fail_count = 0,
    mongo_url = "mongodb://aftership:12qwaszx@ds051893.mongolab.com:51893/exrate?auto_reconnect=true",
    beans_url = "challenge.aftership.net:11300";

if (!module.parent) {
    var mongodb = mongo.db(mongo_url);
    // init beanstalkd and mongodb
    var options = {beansClient : bs.Client(beans_url), mongodb : mongodb}
    notifyWorkers(options);
}

function notifyWorkers(options){
	if(process.argv[2] == 'producer') {
		// run producer worker
		// sudo node --harmony-generators exchangeRateSpider.js producer
		_produceSeed(options);
	} else if(process.argv[2] == 'consumer'){
		// run consumer worker
		// sudo node --harmony-generators exchangeRateSpider.js consumer
		_consumeSeed(options);
	}
}

//produce seed
function _produceSeed(options){
	options.beansClient.use("zegun").onSuccess(function(data) {
		console.log(data);
		var job_data = {
		  "from": "HKD",
		  "to": "USD"
		};
		options.beansClient.put(JSON.stringify(job_data)).onSuccess(function(data){ 
			console.log(data);
			options.beansClient.disconnect();
		});
	});
}

//consume Seed
function _consumeSeed(options){
	options.beansClient.watch("zegun").onSuccess(function(data){
		options.beansClient.reserve().onSuccess(function(job) {
			co(function* () {
				var result = yield _getExRate(options,JSON.parse(job.data));
				return result;
			}).then(function(html){
				$ = cheerio.load(html);
				if($(".rightCol") && $(".rightCol").length > 0 && $(".rightCol").eq(0).text()) {
					var rate = $(".rightCol").eq(0).text().split(" ")[0];
					if(!isNaN(parseFloat(rate)) && isFinite(rate)) {
						rate = parseFloat(rate).toFixed(2);
					}
					saveObj = {
						"from": "HKD",
						"to": "USD",
						"rate" : rate,
						"created_at": moment(Date.now()).format()
					};
					return Promise.resolve(saveObj);
				}
				return Promise.reject("false");
			}).then(function(saveObj) {
		    	if(success_count < 10) {
		    		success_count ++;
		    		console.log(saveObj);
					exchangeRate = options.mongodb.collection('exchangeRate');
					saveMongo(options,saveObj,function(err){
						if(err) {
							return Promise.reject(err);
						} else {
							options.beansClient.put(job.data).onSuccess(function(data) {
							    console.log(data);
							});
							setTimeout(function(){
								return _consumeSeed(options);
							},1000*60);
						}
					});
				} else {
					console.log('process stop');
					process.exit(0);
				}
		    	options.beansClient.deleteJob(job.id).onSuccess(function(del_msg) {
		            console.log('deleted', job);
		        });
			},function(error){
				console.log("error:"+error);
				fail_count ++;
		    	if(fail_count < 3) {
					options.beansClient.put(job.data).onSuccess(function(data) {
					    console.log(data);
					});
					setTimeout(function(){
						return _consumeSeed(options);
					},1000*3);
		    	} else {
		    		console.log('process stop');
		    		process.exit(0);
		    	}
	    		options.beansClient.deleteJob(job.id).onSuccess(function(del_msg) {
		            console.log('deleted', job);
		        });
		    	
			});
		});
	});
}

// get Exchange Rate
function _getExRate(jobObj){
	return new Promise(function(resolve, reject){
		rp('http://www.xe.com/currencyconverter/convert/?Amount=1&From='+jobObj.from+'&To='+jobObj.to).then(function(html){
			return resolve(html);
		}).error(function(e){
			return reject(e);
		}) ;
	});
}


// save result into mongo db
function saveMongo(options,data,callback){
	var collection = options.mongodb.collection("exchangeRate"); 
	console.log("To save into Mongo db");
	collection.save(data,function(err,saveObj){
		if(!err) {
			console.log("save successfully");
		} else {
			console.log("save fail");
		}
		return callback(err);
	});
}