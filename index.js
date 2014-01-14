"use strict";

var url = require("url"),
    zlib = require("zlib");

var mapnik = require("mapnik");

module.exports = function(tilelive, options) {
  // NOTE: this only works against "data" providers
  var CSVTransform = function(uri, callback) {
    uri = url.parse(uri);

    uri.protocol = uri.protocol.replace("csv+", "");

    return tilelive.load(uri, function(err, source) {
      if (err) {
        return callback(err);
      }

      this.source = source;

      // TODO make a getInfo call to validate that this can be converted?

      return callback(null, this);
    }.bind(this));
  };

  CSVTransform.prototype.getInfo = function(callback) {
    // TODO cache this
    return this.source.getInfo(function(err, info) {
      if (err) {
        return callback(err);
      }

      info.format = "csv";

      return callback(null, info);
    });
  };

  // TODO signal NotFound separately from errors
  CSVTransform.prototype.getTile = function(z, x, y, callback) {
    return this.source.getTile(z, x, y, function(err, data, headers) {
      if (err) {
        return callback(err);
      }

      var tile = new mapnik.VectorTile(z, x, y);

      return zlib.inflate(data, function(err, data) {
        if (err) {
          return callback(err);
        }

        return tile.setData(data, function(err) {
          if (err) {
            return callback(err);
          }

          // in theory this would work for any Mapnik data source
          // TODO layers will likely have different sets of attributes, so
          // a single CSV isn't really the right format to output
          var csv = tile.layers().map(function(layer) {
            var features = layer.datasource.featureset(),
                f,
                header,
                rows = [];

            while ((f = features.next(true))) {
              var attrs = f.attributes();

              // TODO presumes that all rows have all keys, which may not be
              // the case
              if (!header) {
                header = ["layer", "wkt"].concat(Object.keys(attrs)).join("\t");
              }

              var row = Object
                .keys(attrs)
                .map(function(key) {
                  return attrs[key];
                });

              rows.push([layer.name, f.toWKT()].concat(row).join("\t"));
            }

            return [header].concat(rows).join("\n");
          }).join("\n");

          // TODO case-insensitive replacement
          headers = {
            "Content-Type": "text/csv"
          };

          return callback(null, csv, headers);
        });
      });
    });
  };

  CSVTransform.registerProtocols = function(tilelive) {
    // TODO wildcarding csv+ would be nice
    tilelive.protocols["csv+file:"] = this;
    tilelive.protocols["csv+mapbox:"] = this;
  };

  CSVTransform.registerProtocols(tilelive);

  return CSVTransform;
};
