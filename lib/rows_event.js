var util = require('util');
var BinlogEvent = require('./binlog_event').BinlogEvent;
var Common = require('./common');

var Version2Events = [
  0x1e, // WRITE_ROWS_EVENT_V2,
  0x1f, // UPDATE_ROWS_EVENT_V2,
  0x20, // DELETE_ROWS_EVENT_V2
];

var CHECKSUM_SIZE = 4;

/**
 * Generic RowsEvent class
 * Attributes:
 *   position: Position inside next binlog
 *   binlogName: Name of next binlog file
 *   zongji: ZongJi instance
 **/

function RowsEvent(parser, options, zongji) {
  BinlogEvent.apply(this, arguments);
  this._zongji = zongji;
  this._readTableId(parser);
  this.flags = parser.parseUnsignedNumber(2);
  this.useChecksum = zongji.useChecksum;

  // Version 2 Events
  if (Version2Events.indexOf(options.eventType) !== -1) {
    this.extraDataLength = parser.parseUnsignedNumber(2);
    // skip extra data
    parser.parseBuffer(this.extraDataLength - 2);
  }

  // Body
  this.numberOfColumns = parser.parseLengthCodedNumber();

  this.tableMap = options.tableMap;

  var tableData = this.tableMap[this.tableId];
  if (tableData === undefined) {
    // TableMap event was filtered
    parser._offset = parser._packetEnd;
    this._filtered = true;
  } else {
    var columnsPresentBitmapSize = Math.floor((this.numberOfColumns + 7) / 8);
    // Columns present bitmap exceeds 4 bytes with >32 rows
    // And is not handled anyways so just skip over its space
    parser._offset += columnsPresentBitmapSize;
    if (this._hasTwoRows) {
      // UpdateRows event slightly different, has new and old rows represented
      parser._offset += columnsPresentBitmapSize;
    }

    if (this.useChecksum) {
      // Ignore the checksum at the end of this packet
      parser._packetEnd -= CHECKSUM_SIZE;
    }

    this.rows = [];
    while (!parser.reachedPacketEnd()) {
      const row = this._fetchOneRow(parser);

      if (this._hasTwoRows) {
        const beforeUpdate = row.before;
        const afterUpdate = row.after;

        // Verifica se algum campo de updateIncludeFields existe na tabela e foi alterado
        const hasRelevantChange = zongji.options.updateIncludeFields?.some((field) => {
          if(hasDifference(beforeUpdate[field], afterUpdate[field])) {
            return true;
          }
        });

        if (hasRelevantChange) {
          this.rows.push({ before: beforeUpdate, after: afterUpdate});
        }
      } else {
        this.rows.push(row);
      }
    }

    if (this.rows.length === 0) {
      this._filtered = true;
    }

    if (this.useChecksum) {
      // Skip past the checksum at the end of the packet
      parser._packetEnd += CHECKSUM_SIZE;
      parser._offset += CHECKSUM_SIZE;
    }
  }
}

const hasDifference = (beforeValue, afterValue) => {
  // Quick equality check first to avoid unnecessary processing
  if (beforeValue === afterValue) return false;
  
  // Handle cases where both values are Date objects
  if (beforeValue instanceof Date && afterValue instanceof Date) {
    return beforeValue.getTime() !== afterValue.getTime();
  }

  // Handle null, undefined, and objects separately
  if ((beforeValue === null || afterValue === null) || 
      (typeof beforeValue === 'object' || typeof afterValue === 'object')) {
    return beforeValue !== afterValue;
  }

  // If none of the above, perform simple inequality check
  return beforeValue !== afterValue;
};

util.inherits(RowsEvent, BinlogEvent);

RowsEvent.prototype.setTableMap = function(tableMap) {
  this.tableMap = tableMap;
};

RowsEvent.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('Affected columns:', this.numberOfColumns);
  console.log('Changed rows:', this.rows.length);
  console.log('Values:');
  this.rows.forEach(function(row) {
    console.log('--');
    Object.keys(row).forEach(function(name) {
      console.log('Column: %s, Value: %s', name, row[name]);
    });
  });
};

RowsEvent.prototype._fetchOneRow = function(parser) {
  return readRow(this.tableMap[this.tableId], parser, this._zongji);
};

var readRow = function(tableMap, parser, zongji) {
  var row = {}, curNullByte;
  var columns = tableMap.columns;
  var columnSchemas = tableMap.columnSchemas;
  var nullBitmapSize = Math.ceil(columns.length / 8);
  var nullBuffer = parser._buffer.slice(parser._offset, parser._offset + nullBitmapSize);
  parser._offset += nullBitmapSize;

  // Pré-processa todos os bytes nulos em uma array
  const nullBytes = Array.from({ length: nullBitmapSize }, (_, i) => nullBuffer.readUInt8(i));

  for (let i = 0; i < columns.length; i++) {
    curNullByte = nullBytes[Math.floor(i / 8)];
    const column = columns[i];
    const columnSchema = columnSchemas[i];

    if ((curNullByte & (1 << (i % 8))) === 0) {
      row[column.name] = Common.readMysqlValue(parser, column, columnSchema, tableMap, zongji);
    } else {
      row[column.name] = null;
    }
  }
  return row;
};

// Subclasses
function WriteRows(parser, options) { // eslint-disable-line
  RowsEvent.apply(this, arguments);
}

util.inherits(WriteRows, RowsEvent);

// eslint
function DeleteRows(parser, options) { // eslint-disable-line
  RowsEvent.apply(this, arguments);
}

util.inherits(DeleteRows, RowsEvent);

function UpdateRows(parser, options) { // eslint-disable-line
  this._hasTwoRows = true;
  RowsEvent.apply(this, arguments);
}

util.inherits(UpdateRows, RowsEvent);

UpdateRows.prototype._fetchOneRow = function(parser) {
  var tableMap = this.tableMap[this.tableId];
  return {
    before: readRow(tableMap, parser, this._zongji),
    after: readRow(tableMap, parser, this._zongji)
  };
};

UpdateRows.prototype.dump = function() {
  BinlogEvent.prototype.dump.apply(this);
  console.log('Affected columns:', this.numberOfColumns);
  console.log('Changed rows:', this.rows.length);
  console.log('Values:');
  this.rows.forEach(function(row) {
    console.log('--');
    Object.keys(row.before).forEach(function(name) {
      console.log('Column: %s, Value: %s => %s', name, row.before[name], row.after[name]);
    });
  });
};

exports.WriteRows = WriteRows;
exports.DeleteRows = DeleteRows;
exports.UpdateRows = UpdateRows;
