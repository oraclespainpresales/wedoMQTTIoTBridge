'use strict';

// Module imports
const Device = require('./device')
    , iotcs = require('./device-library.node')({debug: false})
    , async = require('async')
    , _ = require('lodash')
    , restify = require('restify-clients')
    , mqtt = require('mqtt')
    , queue = require('block-queue')
    , log = require('npmlog-ts')
    , config = require('config')
    , util = require('util')
    , fs = require('fs-extra')
    , glob = require("glob")
    , isUUID = require('is-uuid')
;

log.timestamp = true;
log.level = 'verbose';

//iotcs = iotcs({debug: false});

// Main constants
const PROCESSNAME = "WEDO - IoTCS MQTT Bridge"
    , VERSION = "v1.0"
    , AUTHOR  = "Carlos Casares <carlos.casares@oracle.com>"
    , PROCESS = 'PROCESS'
    , CONFIG  = 'CONFIG'
    , IOTCS   = 'IOTCS'
    , REST    = "REST"
    , QUEUE   = "QUEUE"
    , MQTT    = "MQTT"
    , DATA    = "DATA"
    , ALERT   = "ALERT"
    , IOTFOLDER = './iot'
;

// Config "constants"
let APEXHOST
  , APEXBASEURI
  , IOTHOST
  , IOTAVAILABILITYRETRIES
  , IOTAVAILABILITYPAUSE
  , IOTUSAGETIMEOUT
  , MQTTBROKER
  , MQTTUSERNAME
  , MQTTPASSWORD
  , MQTTRECONNECTPERIOD
  , MQTTCONNECTTIMEOUT
  , MQTTGLOBALTOPIC
  , MQTTQOS
;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  console.log("Uncaught Exception: " + err);
  console.log("Uncaught Exception: " + err.stack);
});
process.on('SIGINT', function() {
  log.info(PROCESS, "Caught interrupt signal");
  log.info(PROCESS, "Exiting gracefully");
  process.removeAllListeners()
  if (typeof err != 'undefined')
    log.error(PROCESS, err)
  process.exit(2);
});
// Main handlers registration - END

// Initializing REST client BEGIN
var dbClient = _.noop();
// Initializing REST client END

// Initializing QUEUE variables BEGIN
var q = _.noop();
const QUEUECONCURRENCY = 1;
// Initializing QUEUE variables END

// Misc vars
var configData = _.noop()
  , mqttClient = _.noop()
  , DEVICES = []
;

// Main sync flow
async.series({
  splash: (next) => {
    log.info(PROCESS, "%s - %s", PROCESSNAME, VERSION);
    log.info(PROCESS, "Author - %s", AUTHOR);
    next();
  },
  config: (next) => {
    // Get and validate configuration stuff
    try {
      APEXHOST = config.get('configdb.host');
      APEXBASEURI = config.get('configdb.baseuri');
      IOTHOST = config.get('iot.host');
      IOTAVAILABILITYRETRIES = config.get('iot.availability.retries');
      IOTAVAILABILITYPAUSE = config.get('iot.availability.pause');
      IOTUSAGETIMEOUT = config.get('iot.usage.timeout');
      MQTTGLOBALTOPIC = config.get('mqtt.globalTopic');
      MQTTQOS = config.get('mqtt.qos');

      if ( _.isUndefined(APEXHOST) || _.isUndefined(APEXBASEURI) || _.isUndefined(IOTHOST) || _.isUndefined(IOTAVAILABILITYRETRIES) || _.isUndefined(IOTAVAILABILITYPAUSE) ||
           _.isUndefined(IOTUSAGETIMEOUT) || _.isUndefined(MQTTGLOBALTOPIC) || _.isUndefined(MQTTQOS)) {
        throw new Error("Empty value in config file");
      }
      if ( isNaN(IOTAVAILABILITYRETRIES) || isNaN(IOTAVAILABILITYPAUSE) || isNaN(IOTUSAGETIMEOUT) ||
           isNaN(MQTTRECONNECTPERIOD) || isNaN(MQTTCONNECTTIMEOUT) || isNaN(MQTTQOS)) {
        throw new Error("Invalid numeric value");
      }

      next();
    } catch (e) {
      next(new Error(util.format("Invalid config file: %s", e.message)));
    }
  },
  deviceConfig: (next) => {
    // Get database config for all demos and demozones
    log.verbose(DATA, "Retrieving device info from database...");
    dbClient = restify.createJsonClient({
      url: APEXHOST,
      connectTimeout: 10000,
      requestTimeout: 10000,
      retry: false,
      rejectUnauthorized: false,
      headers: {
        "content-type": "application/json",
        "accept": "application/json"
      }
    });

    const DEVICESURI = "/devices";

    dbClient.get(APEXBASEURI + DEVICESURI, function(err, req, res, obj) {
      if (err) {
        next(new Error(util.format("Error retrieving Devices setup: %s", err.message)));
        return;
      }
      configData = JSON.parse(res.body).items;
      if (configData.length == 0) {
        next(new Error("No active devices found in DB. Aborting"));
        return;
      }
      next();
    });
  },
  mqttConfig: (next) => {
    const MQTTSETUPURI = "/setup/mqtt";
    log.verbose(DATA, "Retrieving MQTT broker setup info from database...");
    dbClient.get(APEXBASEURI + MQTTSETUPURI, function(err, req, res, obj) {
      if (err) {
        next(new Error(util.format("Error retrieving MQTT broker setup: %s", err.message)));
        return;
      }
      var config = JSON.parse(res.body);
      MQTTBROKER = config.broker;
      MQTTUSERNAME = config.username;
      MQTTPASSWORD = config.password;
      MQTTRECONNECTPERIOD = config.reconnectperiod;
      MQTTCONNECTTIMEOUT = config.connecttimeout;
      next();
    });
  },
  queue: (next) => {
    log.info(QUEUE, "Initializing QUEUE system");
    q = queue(QUEUECONCURRENCY, (msg, done) => {
      log.verbose(QUEUE, "Dequeued message: %j", msg);
      // Search device by device id
      let d = _.find(DEVICES, { deviceid: msg.deviceid });
      if (!d) {
        log.error(QUEUE, "No device found for device id '%s'. Ignoring.", msg.deviceid);
        done();
        return;
      }
      // Check if demo and demozone matches the device found and urn is registered
      if (d.demo.toUpperCase() !== msg.demo.toUpperCase()) {
        log.error(QUEUE, "Device demo '%s' does not match topic demo '%s'. Ignoring", d.demo.toUpperCase(), msg.demo.toUpperCase());
        done();
        return;
      }
      if (d.demozone.toUpperCase() !== msg.demozone.toUpperCase()) {
        log.error(QUEUE, "Device demozone '%s' does not match topic demozone '%s'. Ignoring", d.demozone.toUpperCase(), msg.demozone.toUpperCase());
        done();
        return;
      }
      if (!d.device.isValidUrn(msg.payload.type, msg.payload.urn)) {
        log.error(QUEUE, "Message type of '%s' and URN '%s' not found in device registration. Ignoring", msg.payload.type.toUpperCase(), msg.payload.urn);
        done();
        return;
      }
      // Check finished
      // Check now IoTCS device
      async.series({
        check: (cont) => {
          if (!d.device.isValid()) {
            // Device is not yet connected
            d.device.connect(IOTUSAGETIMEOUT)
            .then(() => {cont()})
            .catch((err) => {done(); return;})
          } else {
            cont();
          }
        },
        send: (cont) => {
          if (msg.payload.type.toUpperCase() == 'DATA') {
            d.device.sendData(msg.payload.urn, msg.payload.payload);
          } else {
            d.device.sendAlert(msg.payload.urn, msg.payload.payload);
          }
          cont();
        }
      }, (err) => {
        if (err) {
          log.error(PROCESS, err);
        }
        done(); // Let queue handle next task
      });
    });
    log.info(QUEUE, "QUEUE system initialized successfully");
    next();
  },
  iot: (next) => {
    log.info(PROCESS, "Initializing IoT devices...");
    // Create the folder and ignore any error if it exists already
    try {fs.mkdirSync(IOTFOLDER)} catch(e){};
    // Iterate over all devices read from DB, create the device and push it to the main device array. DO NOT CONNECT YET. Connection will be done when first message arrives
    glob(IOTFOLDER + '/' + '*.conf', (er, files) => {
      _.forEach(files, (f) => {
        fs.removeSync(f);
      });
      _.each(configData, (d) => {
        let entry = {
          demo: d.demo,
          demozone: d.demozone,
          devicename: d.devicename,
          deviceid: d.deviceid,
          mqtttopic: d.mqtttopic
        }
        log.info(PROCESS, "'%s' demo, '%s' demozone, '%s' device...", entry.demo, entry.demozone, entry.devicename);
        // Refresh or create the provisioning data conf file
        let provisioningFilename = IOTFOLDER + '/' + entry.demo.toUpperCase() + '.' + entry.devicename.toUpperCase() + '.' + entry.demozone.toUpperCase() + '.conf';
        fs.removeSync(provisioningFilename); // No exception if it does not exists
        fs.writeFileSync(provisioningFilename, d.provisiondata, 'utf8');
        let alerts = d.alerts ? JSON.parse(d.alerts) : _.noop();
        let _device = new Device(log, iotcs, entry.deviceid, provisioningFilename, d.provisionpassword, JSON.parse(d.urns), alerts);
        entry.device = _device;
        DEVICES.push(entry);
      });
      log.verbose(PROCESS, "IoT devices initialized successfully");
      next();
    });
  },
  mqtt: (next) => {
    // Initialize mqtt
    log.info(MQTT, "Connecting to MQTT broker at %s", MQTTBROKER);
    mqttClient  = mqtt.connect(MQTTBROKER, { username: MQTTUSERNAME, password: MQTTPASSWORD, reconnectPeriod: MQTTRECONNECTPERIOD, connectTimeout: MQTTCONNECTTIMEOUT });
    mqttClient.connected = false;

    // Common event handlers
    mqttClient.on('connect', () => {
      log.info(MQTT, "Successfully connected to MQTT broker at %s", MQTTBROKER);
      mqttClient.connected = true;
    });

    mqttClient.on('error', err => {
      log.error(MQTT, "Error: ", err);
    });

    mqttClient.on('reconnect', () => {
      log.verbose(MQTT, "Client trying to reconnect...");
    });

    mqttClient.on('offline', () => {
      mqttClient.connected = false;
      log.warn(MQTT, "Client went offline!");
    });

    mqttClient.on('end', () => {
      mqttClient.connected = false;
      log.info(MQTT, "Client ended");
    });

    // Message subscriber & event handler
    mqttClient.subscribe(MQTTGLOBALTOPIC, (err, granted) => {
      if (!err) {
        log.verbose(MQTT, "Subscribed to topic '%s' with allowed QoS of: %d", granted[0].topic, granted[0].qos);
      } else {
        log.error(MQTT, "Unable to subscribe to topic '%s': ", MQTTGLOBALTOPIC, err);
      }
    })

    mqttClient.on('message', (topic, message) => {
      let strMessage = message.toString();
      log.verbose(MQTT, "Message received with topic '%s' and data: %s", topic, strMessage);
      // Split out the incoming topic
      // Must be of format:
      // wedo/{demo}/{demozone}/{device name}/{device id}
      // [0]    [1]      [2]         [3]          [4]
      let t = topic.split('/');
      // First validate whether device id is present and a valid UUID v4
      if (!isUUID.v4(t[4])) {
        log.error(MQTT, "Invalid topic: unknown device id format: '%s'. Ignoring message", t[4]);
        return;
      }
      try {
        var payload = JSON.parse(strMessage);
        if (!payload.type || (payload.type.toUpperCase() !== "DATA" && payload.type.toUpperCase() !== "ALERT") || !payload.urn || !payload.payload ) {
          throw new Error();
        }
      } catch (e) {
        log.error(MQTT, "Invalid payload format. Ignoring message");
        return;
      }
      let msg = {
        demo: t[1],
        demozone: t[2],
        devicename: t[3],
        deviceid: t[4],
        payload: payload
      };
      q.push(msg);
    })
    next();
  },
}, (err, results) => {
  if (err) {
    log.error(PROCESS, err.message);
    log.error(PROCESS, "Aborting");
    process.exit(1);
  }
  log.info(PROCESS, "Initialization completed");
});
