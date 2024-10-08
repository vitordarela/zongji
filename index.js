var mysql = require('mysql');
var Connection = require('mysql/lib/Connection');
var Pool = require('mysql/lib/Pool');

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var generateBinlog = require('./lib/sequence/binlog');

var alternateDsn = [
  { type: Connection, config: function(obj) { return obj.config; } },
  { type: Pool, config: function(obj) { return obj.config.connectionConfig; } }
];

function ZongJi(dsn, options) {
  this.set(options);

  EventEmitter.call(this);

  var binlogDsn;

  // one connection to send table info query
  // Check first argument against possible connection objects
  for (var i = 0; i < alternateDsn.length; i++) {
    if (dsn.constructor.name === alternateDsn[i].type.name) {
      this.ctrlConnection = dsn;
      this.ctrlConnectionOwner = false;
      binlogDsn = cloneObjectSimple(alternateDsn[i].config(dsn));
    }
  }

  if (!binlogDsn) {
    // assuming that the object passed is the connection settings
    var ctrlDsn = cloneObjectSimple(dsn);
    this.ctrlConnection = mysql.createConnection(ctrlDsn);
    this.ctrlConnection.on('error', this._emitError.bind(this));
    this.ctrlConnection.on('unhandledError', this._emitError.bind(this));
    this.ctrlConnection.connect();
    this.ctrlConnectionOwner = true;

    binlogDsn = dsn;
  }
  this.ctrlCallbacks = [];

  this.connection = mysql.createConnection(binlogDsn);
  this.connection.on('error', this._emitError.bind(this));
  this.connection.on('unhandledError', this._emitError.bind(this));

  this.tableMap = {};
  this.ready = false;
  this.useChecksum = false;
  // Include 'rotate' events to keep these properties updated
  this.binlogName = null;
  this.binlogNextPos = null;

  this._init();
}

var cloneObjectSimple = function(obj) {
  var out = {};
  for (var i in obj) {
    if (obj.hasOwnProperty(i)) {
      out[i] = obj[i];
    }
  }
  return out;
};

util.inherits(ZongJi, EventEmitter);

ZongJi.prototype._init = function() {
  var self = this;
  var binlogOptions = {
    tableMap: self.tableMap,
  };

  var asyncMethods = [
    {
      name: '_isChecksumEnabled',
      callback: function(checksumEnabled) {
        self.useChecksum = checksumEnabled;
        binlogOptions.useChecksum = checksumEnabled;
      }
    },
    {
      name: '_findBinlogEnd',
      callback: function(result) {
        if (result && self.options.startAtEnd) {
          binlogOptions.filename = result.Log_name;
          binlogOptions.position = result.File_size;
        }
      }
    }
  ];

  var methodIndex = 0;
  var nextMethod = function() {
    var method = asyncMethods[methodIndex];
    self[method.name](function(/* args */) {
      method.callback.apply(this, arguments);
      methodIndex++;
      if (methodIndex < asyncMethods.length) {
        nextMethod();
      }
      else {
        ready();
      }
    });
  };
  nextMethod();

  var ready = function() {
    // Run asynchronously from _init(), as serverId option set in start()
    if (self.options.serverId !== undefined) {
      binlogOptions.serverId = self.options.serverId;
    }

    if (('binlogName' in self.options) && ('binlogNextPos' in self.options)) {
      binlogOptions.filename = self.options.binlogName;
      binlogOptions.position = self.options.binlogNextPos;
    }

    self.binlog = generateBinlog.call(self, binlogOptions);
    self.ready = true;
    self._executeCtrlCallbacks();
  };
};

ZongJi.prototype._isChecksumEnabled = function(next) {
  var self = this;
  var sql = 'select @@GLOBAL.binlog_checksum as checksum';
  var ctrlConnection = self.ctrlConnection;
  var connection = self.connection;

  ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      if (err.toString().match(/ER_UNKNOWN_SYSTEM_VARIABLE/)) {
        // MySQL < 5.6.2 does not support @@GLOBAL.binlog_checksum
        return next(false);
      } else {
        // Any other errors should be emitted
        self.emit('error', err);
        return;
      }
    }

    var checksumEnabled = true;
    if (rows[0].checksum === 'NONE') {
      checksumEnabled = false;
    }

    var setChecksumSql = 'set @master_binlog_checksum=@@global.binlog_checksum';
    if (checksumEnabled) {
      connection.query(setChecksumSql, function(err) {
        if (err) {
          // Errors should be emitted
          self.emit('error', err);
          return;
        }
        next(checksumEnabled);
      });
    } else {
      next(checksumEnabled);
    }
  });
};

ZongJi.prototype._findBinlogEnd = function(next) {
  var self = this;
  self.ctrlConnection.query('SHOW BINARY LOGS', function(err, rows) {
    if (err) {
      // Errors should be emitted
      self.emit('error', err);
      return;
    }
    next(rows.length > 0 ? rows[rows.length - 1] : null);
  });
};

ZongJi.prototype._executeCtrlCallbacks = function() {
  if (this.ctrlCallbacks.length > 0) {
    this.ctrlCallbacks.forEach(function(cb) {
      setImmediate(cb);
    });
  }
};

var tableInfoQueryTemplateWithSchema = `
SELECT
  COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
  COLUMN_COMMENT, COLUMN_TYPE
FROM
  information_schema.columns
WHERE
  table_schema='%s' AND table_name='%s'
ORDER BY ORDINAL_POSITION;
`;

var tableInfoQueryTemplateWithoutSchema = `
SELECT
  COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
  COLUMN_COMMENT, COLUMN_TYPE
FROM
  information_schema.columns
WHERE
  table_name='%s'
ORDER BY ORDINAL_POSITION;
`;

ZongJi.prototype._fetchTableInfo = function(tableMapEvent, next) {
  var self = this;
  
  var sql;
  if (tableMapEvent.schemaName && tableMapEvent.schemaName !== '*') {
    sql = util.format(tableInfoQueryTemplateWithSchema,
      tableMapEvent.schemaName, tableMapEvent.tableName);
  } else {
    sql = util.format(tableInfoQueryTemplateWithoutSchema,
      tableMapEvent.tableName);
  }

  this.ctrlConnection.query(sql, function(err, rows) {
    if (err) {
      // Erros devem ser emitidos
      self.emit('error', err);
      return;
    }

    if (rows.length === 0) {
      self.emit('error', new Error(
        'Permissões insuficientes para acessar: ' +
        (tableMapEvent.schemaName ? (tableMapEvent.schemaName + '.') : '') + tableMapEvent.tableName +
        ', ou esta tabela foi removida.'));
      return next();
    }

    self.tableMap[tableMapEvent.tableId] = {
      columnSchemas: rows,
      parentSchema: tableMapEvent.schemaName || null, // Deixe null se não houver schema
      tableName: tableMapEvent.tableName
    };

    next();
  });
};

ZongJi.prototype.set = function(options) {
  this.options = options || {};
};

ZongJi.prototype.start = function(options) {
  var self = this;
  self.set(options);

  var _start = function() {
    self.connection._implyConnect();
    self.connection._protocol._enqueue(new self.binlog(function(error, event) {
      if (error) return self.emit('error', error);
      // Do not emit events that have been filtered out
      if (event === undefined || event._filtered === true) return;

      switch (event.getTypeName()) {
        case 'TableMap':
          var tableMap = self.tableMap[event.tableId];

          if (!tableMap) {
            self.connection.pause();
            self._fetchTableInfo(event, function() {
              // merge the column info with metadata
              event.updateColumnInfo();
              self.emit('binlog', event);
              self.connection.resume();
            });
            return;
          }
          break;
        case 'Rotate':
          if (self.binlogName !== event.binlogName) {
            self.binlogName = event.binlogName;
          }
          break;
      }
      self.binlogNextPos = event.nextPosition;
      self.emit('binlog', event);
    }));
  };

  if (this.ready) {
    _start();
  }
  else {
    this.ctrlCallbacks.push(_start);
  }
};

ZongJi.prototype.stop = function() {
  var self = this;
  // Binary log connection does not end with destroy()
  self.connection.destroy();
  self.ctrlConnection.query(
    'KILL ' + self.connection.threadId,
    function() {
      if (self.ctrlConnectionOwner)
        self.ctrlConnection.destroy();
    }
  );
};

ZongJi.prototype._skipEvent = function(eventName) {
  var include = this.options.includeEvents;
  var exclude = this.options.excludeEvents;
  return !(
   (include === undefined ||
    (include instanceof Array && include.indexOf(eventName) !== -1)) &&
   (exclude === undefined ||
    (exclude instanceof Array && exclude.indexOf(eventName) === -1)));
};

ZongJi.prototype._skipSchema = function(database, table) {
  var include = this.options.includeSchema;
  var exclude = this.options.excludeSchema;

  // Verifica se includeSchema tem um schema "*" e ignora o schema
  var includeAllSchemas = include && '*' in include && (include['*'] === true || (include['*'] instanceof Array && include['*'].indexOf(table) !== -1));

  return !(
    (
      // Se o include tiver "*" ou o database específico
      include === undefined ||
      includeAllSchemas || // Se estamos incluindo todas as tabelas do schema "*"
      (database !== undefined && (database in include) &&
        (include[database] === true ||
         (include[database] instanceof Array &&
          include[database].indexOf(table) !== -1)))
    ) &&
    (
      exclude === undefined ||
      (
        database !== undefined &&
        (!(database in exclude) ||
         (exclude[database] !== true &&
           (exclude[database] instanceof Array &&
            exclude[database].indexOf(table) === -1))))
    )
  );
};

ZongJi.prototype._emitError = function(error) {
  this.emit('error', error);
};

module.exports = ZongJi;
