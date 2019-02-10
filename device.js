"use strict";

const async = require('async')
    , _ = require('lodash')
;

const IOTCS = 'IOTCS'
;

class Device {
  constructor(log, iotcs, id, trustedAssetsStoreFile, trustedAssetsStorePassword, urn, alerts = undefined) {
    this._log = log;
    this._iotcs = iotcs;
    this._id = id;
    this._urn = urn;
    this._VD = [];
    if (alerts) this._alerts = alerts;
    this._trustedAssetsStoreFile = trustedAssetsStoreFile;
    this._trustedAssetsStorePassword = trustedAssetsStorePassword;
    this._valid = false;
  }

  isValid() {
    return this._valid;
  }

  isValidUrn(type, urn) {
    if (type.toLowerCase() == 'data') {
      return _.includes(this._urn, urn);
    } else {
      return (_.find(this._alerts, { alertUrn: urn }) !== _.noop());
    }
  }

  initialize() {
    return new Promise((resolve, reject) => {
      this._DCD = new this._iotcs.device.DirectlyConnectedDevice(this._trustedAssetsStoreFile, this._trustedAssetsStorePassword);
      async.series({
        activate: (next) => {
          if (!this._DCD.isActivated()) {

          } else {
            next();
          }
        },
        models: (next) => {
          // Iterate over all URNs
          async.eachSeries(this._urn, (urn, nextUrn) => {
            this._DCD.getDeviceModel(urn, (model, err) => {
              if (!err) {
                var vd = this._DCD.createVirtualDevice(this._DCD.getEndpointId(), model);
                this._VD.push({
                  urn: urn,
                  vd: vd
                });
                this._log.verbose(IOTCS, "'" + urn + "' intialized successfully");
                nextUrn();
              } else {
                nextUrn(err);
              }
            });
          }, (err) => {
            next(err);
          });
        }
      }, (err, results) => {
        err ? reject(err) : resolve();
      });
    });
  }

  disconnect() {
    this._log.verbose(IOTCS, "Disconnecting device: " + this._id);
    this._valid = false;
    if (this._timeoutHandler) {
      clearTimeout(this._timeoutHandler);
    }
    if (this._DCD) {
      _.each(this._VD, (v) => {
        v.vd.close();
      });
      this._VD.length = 0;
      this._DCD.close();
      delete this._DCD;
    }
  }

  setTimeout() {
    if (this._timeoutHandler) {
      clearTimeout(this._timeoutHandler);
    }
    this._timeoutHandler = setTimeout(() => {
      this._timeoutHandler = _.noop();
      this._log.verbose(IOTCS, "No usage timeout reached");
      this.disconnect();
    }, this._timeout * 60 * 1000);
  }

  connect(timeout) {
    return new Promise((resolve, reject) => {
      this._log.verbose(IOTCS, "Connecting device '%s'...", this._id);
      this._timeout = timeout;
      this.initialize()
      .then(() => {
        this._log.verbose(IOTCS, "Device '%s' connected", this._id);
        this._valid = true;
        this.setTimeout();
        resolve();
      })
      .catch((err) => {
        this._log.verbose(IOTCS, "ERROR: " + err);
        reject(err);
      });
    });
  }

  sendData(urn, data) {
    let v = _.find(this._VD, { urn: urn } );
    if (v) {
      this.setTimeout();
      v.vd.update(data);
    } else {
      this._log.error(IOTCS, "VD not found for given urn '%s'", urn);
    }
  }

  sendAlert(urn, data) {
    if (!this._alerts) {
      this._log.error(IOTCS, "No alerts registered for this device");
      return;
    }
    let u = _.find(this._alerts, { alertUrn: urn } );
    if (!u) {
      this._log.error(IOTCS, "Alert URN '%s' not registered for this device", urn);
      return;
    }
    let v = _.find(this._VD, { urn: u.deviceUrn } );
    if (!v) {
      this._log.error(IOTCS, "Device associated for alert URN '%s' not found!", urn);
      return;
    }
    let alert = v.vd.createAlert(urn);
    Object.keys(data).forEach((key) => {
      alert.fields[key] = data[key];
    });
    this.setTimeout();
    alert.raise();
//    this._log.verbose(IOTCS, "%s alert raised with data %j", urn, data);
  }

}

module.exports = Device;
