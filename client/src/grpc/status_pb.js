/**
 * status_pb.js
 * Manual protobuf encode/decode for StatusRequest and StatusResponse.
 * No dependency on google-protobuf internals — plain typed arrays only.
 *
 * `message` on both StatusRequest and StatusResponse is a
 * `google.protobuf.Struct` (i.e. arbitrary JSON), not a string — getMessage()/
 * setMessage() work with plain JS values (objects, arrays, strings, numbers,
 * booleans, null), and this file encodes/decodes the Struct/Value/ListValue
 * wire format by hand (field numbers 1–6 are fixed by the well-known type
 * spec: https://protobuf.dev/reference/protobuf/google.protobuf/#struct).
 */

// ── varint helpers ────────────────────────────────────────────────────────────

function encodeVarint(value) {
  const bytes = [];
  let v = value >>> 0;
  while (v > 127) {
    bytes.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  bytes.push(v);
  return new Uint8Array(bytes);
}

function readVarint(buf, offset) {
  let result = 0, shift = 0;
  while (offset < buf.length) {
    const b = buf[offset++];
    result |= (b & 0x7f) << shift;
    if (!(b & 0x80)) break;
    shift += 7;
  }
  return { value: result, offset };
}

// ── field encoders ────────────────────────────────────────────────────────────

function encodeLengthDelimitedField(fieldNumber, bytes) {
  const tag = encodeVarint((fieldNumber << 3) | 2);  // wire type 2
  const len = encodeVarint(bytes.length);
  const out = new Uint8Array(tag.length + len.length + bytes.length);
  out.set(tag, 0);
  out.set(len, tag.length);
  out.set(bytes, tag.length + len.length);
  return out;
}

function encodeStringField(fieldNumber, str) {
  return encodeLengthDelimitedField(fieldNumber, new TextEncoder().encode(str));
}

function encodeVarintField(fieldNumber, value) {
  const tag = encodeVarint((fieldNumber << 3) | 0);  // wire type 0
  const val = encodeVarint(value);
  const out = new Uint8Array(tag.length + val.length);
  out.set(tag, 0);
  out.set(val, tag.length);
  return out;
}

function encodeInt32Field(fieldNumber, value) {
  return encodeVarintField(fieldNumber, value);
}

function encodeDoubleField(fieldNumber, value) {
  const tag = encodeVarint((fieldNumber << 3) | 1);  // wire type 1 (fixed64)
  const buf = new ArrayBuffer(8);
  new DataView(buf).setFloat64(0, value, true);      // little-endian
  const out = new Uint8Array(tag.length + 8);
  out.set(tag, 0);
  out.set(new Uint8Array(buf), tag.length);
  return out;
}

function concat(...arrays) {
  const total  = arrays.reduce((n, a) => n + a.length, 0);
  const result = new Uint8Array(total);
  let offset   = 0;
  for (const a of arrays) { result.set(a, offset); offset += a.length; }
  return result;
}

// ── google.protobuf.Struct encode/decode ─────────────────────────────────────

function encodeValue(v) {
  if (v === null || v === undefined) return encodeVarintField(1, 0); // null_value = NULL_VALUE
  if (typeof v === 'number')  return encodeDoubleField(2, v);         // number_value
  if (typeof v === 'string')  return encodeStringField(3, v);         // string_value
  if (typeof v === 'boolean') return encodeVarintField(4, v ? 1 : 0); // bool_value
  if (Array.isArray(v)) {
    const items = v.map((item) => encodeLengthDelimitedField(1, encodeValue(item)));
    return encodeLengthDelimitedField(6, concat(...items));           // list_value
  }
  if (typeof v === 'object') return encodeLengthDelimitedField(5, encodeStruct(v)); // struct_value
  return encodeVarintField(1, 0);
}

function encodeStruct(obj) {
  const entries = Object.keys(obj || {}).map((key) => {
    const entry = concat(encodeStringField(1, key), encodeLengthDelimitedField(2, encodeValue(obj[key])));
    return encodeLengthDelimitedField(1, entry); // repeated MapEntry on field "fields" = 1
  });
  return concat(...entries);
}

function decodeValue(buf) {
  let offset = 0;
  let result = null;
  while (offset < buf.length) {
    const tagInfo = readVarint(buf, offset); offset = tagInfo.offset;
    const field = tagInfo.value >> 3;
    const wire  = tagInfo.value & 0x07;
    if (wire === 0) {
      const v = readVarint(buf, offset); offset = v.offset;
      if (field === 4) result = v.value !== 0; // bool_value
      // field 1 (null_value) → result stays null
    } else if (wire === 1) {
      const view = new DataView(buf.buffer, buf.byteOffset + offset, 8);
      if (field === 2) result = view.getFloat64(0, true); // number_value
      offset += 8;
    } else if (wire === 2) {
      const lenInfo = readVarint(buf, offset); offset = lenInfo.offset;
      const data = buf.slice(offset, offset + lenInfo.value); offset += lenInfo.value;
      if (field === 3) result = new TextDecoder().decode(data);        // string_value
      if (field === 5) result = decodeStruct(data);                    // struct_value
      if (field === 6) result = decodeListValue(data);                 // list_value
    }
  }
  return result;
}

function decodeListValue(buf) {
  const out = [];
  let offset = 0;
  while (offset < buf.length) {
    const tagInfo = readVarint(buf, offset); offset = tagInfo.offset;
    const field = tagInfo.value >> 3;
    const wire  = tagInfo.value & 0x07;
    if (wire === 2) {
      const lenInfo = readVarint(buf, offset); offset = lenInfo.offset;
      const data = buf.slice(offset, offset + lenInfo.value); offset += lenInfo.value;
      if (field === 1) out.push(decodeValue(data));
    }
  }
  return out;
}

function decodeStruct(buf) {
  const out = {};
  let offset = 0;
  while (offset < buf.length) {
    const tagInfo = readVarint(buf, offset); offset = tagInfo.offset;
    const field = tagInfo.value >> 3;
    const wire  = tagInfo.value & 0x07;
    if (wire === 2) {
      const lenInfo = readVarint(buf, offset); offset = lenInfo.offset;
      const entryData = buf.slice(offset, offset + lenInfo.value); offset += lenInfo.value;
      if (field === 1) {
        let key = '';
        let val = null;
        let eOffset = 0;
        while (eOffset < entryData.length) {
          const eTag = readVarint(entryData, eOffset); eOffset = eTag.offset;
          const eField = eTag.value >> 3;
          const eWire  = eTag.value & 0x07;
          if (eWire === 2) {
            const eLen  = readVarint(entryData, eOffset); eOffset = eLen.offset;
            const eData = entryData.slice(eOffset, eOffset + eLen.value); eOffset += eLen.value;
            if (eField === 1) key = new TextDecoder().decode(eData);
            if (eField === 2) val = decodeValue(eData);
          }
        }
        out[key] = val;
      }
    }
  }
  return out;
}

// ── StatusRequest ─────────────────────────────────────────────────────────────

export class StatusRequest {
  constructor() {
    this._message = null;
  }

  getMessage()       { return this._message; }
  setMessage(value)  { this._message = value; return this; }

  serializeBinary() {
    if (this._message === null || this._message === undefined) return new Uint8Array(0);
    return encodeLengthDelimitedField(1, encodeStruct(this._message));
  }

  static deserializeBinary(bytes) {
    const buf = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    const msg = new StatusRequest();
    let offset = 0;
    while (offset < buf.length) {
      const tagInfo = readVarint(buf, offset);
      offset        = tagInfo.offset;
      const field   = tagInfo.value >> 3;
      const wire    = tagInfo.value & 0x07;
      if (wire === 2) {
        const lenInfo = readVarint(buf, offset);
        offset        = lenInfo.offset;
        const data    = buf.slice(offset, offset + lenInfo.value);
        offset       += lenInfo.value;
        if (field === 1) msg.setMessage(decodeStruct(data));
      }
    }
    return msg;
  }
}

// ── StatusResponse ────────────────────────────────────────────────────────────

export class StatusResponse {
  constructor() {
    this._code    = 0;
    this._status  = '';
    this._message = null;
  }

  getCode()          { return this._code; }
  setCode(value)     { this._code = value; return this; }

  getStatus()        { return this._status; }
  setStatus(value)   { this._status = value; return this; }

  getMessage()       { return this._message; }
  setMessage(value)  { this._message = value; return this; }

  serializeBinary() {
    const parts = [];
    if (this._code    !== 0)  parts.push(encodeInt32Field(1, this._code));
    if (this._status  !== '') parts.push(encodeStringField(2, this._status));
    if (this._message !== null && this._message !== undefined) {
      parts.push(encodeLengthDelimitedField(3, encodeStruct(this._message)));
    }
    return concat(...parts);
  }

  static deserializeBinary(bytes) {
    const buf = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    const msg = new StatusResponse();
    let offset = 0;
    while (offset < buf.length) {
      const tagInfo = readVarint(buf, offset);
      offset        = tagInfo.offset;
      const field   = tagInfo.value >> 3;
      const wire    = tagInfo.value & 0x07;
      if (wire === 0) {
        const valInfo = readVarint(buf, offset);
        offset        = valInfo.offset;
        if (field === 1) msg.setCode(valInfo.value);
      } else if (wire === 2) {
        const lenInfo = readVarint(buf, offset);
        offset        = lenInfo.offset;
        const data    = buf.slice(offset, offset + lenInfo.value);
        offset       += lenInfo.value;
        if (field === 2) msg.setStatus(new TextDecoder().decode(data));
        if (field === 3) msg.setMessage(decodeStruct(data));
      }
    }
    return msg;
  }
}
