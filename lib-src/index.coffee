# Licensed under the Apache License. See footer for details.

Debug = !false

_       = require "underscore"
async   = require "async"
mongodb = require "mongodb"

AbstractLeveldown = require "abstract-leveldown"

#-------------------------------------------------------------------------------
# keys and values always stored as hex-encoded Buffers
#    if a key/value is a String, type: "s", value = new Buffer(the-string)
#    if a key/value is a Buffer, type: "b", value = the-buffer
#    else                        type: "j", value = new Buffer(JSON.stringify(the-thing))
#
# implication is that we store a "type" with each key and val, and encode/decode
# when coming out of mongodb
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# abstract-leveldown converts keys and values to strings! Stop it!
#-------------------------------------------------------------------------------
AbstractLeveldown.AbstractLevelDOWN::get = (key, options, callback) ->
  if (typeof options == 'function')
    callback = options

  if (typeof callback != 'function')
    throw new Error('get() requires a callback argument')

  if (err = this._checkKeyValue(key, 'key', this._isBuffer))
    return callback(err)

  #if (!this._isBuffer(key))
  #  key = String(key)

  if (typeof options != 'object')
    options = {}

  if (typeof this._get == 'function')
    return this._get(key, options, callback)

  process.nextTick(-> callback(new Error('NotFound')))

#-------------------------------------------------------------------------------
# abstract-leveldown converts keys and values to strings! Stop it!
#-------------------------------------------------------------------------------
AbstractLeveldown.AbstractLevelDOWN::put = (key, value, options, callback) ->

  if (typeof options == 'function')
    callback = options

  if (typeof callback != 'function')
    throw new Error('put() requires a callback argument')

  if (err = this._checkKeyValue(key, 'key', this._isBuffer))
    return callback(err)

  if (err = this._checkKeyValue(value, 'value', this._isBuffer))
    return callback(err)

  #if (!this._isBuffer(key))
  #  key = String(key)

  # coerce value to string in node, don't touch it in browser
  # (indexeddb can store any JS type)
  #if (!this._isBuffer(value) && !process.browser)
  #  value = String(value)

  if (typeof options != 'object')
    options = {}

  if (typeof this._put == 'function')
    return this._put(key, value, options, callback)

  process.nextTick(callback)

#-------------------------------------------------------------------------------
# abstract-leveldown converts keys and values to strings! Stop it!
#-------------------------------------------------------------------------------
AbstractLeveldown.AbstractLevelDOWN::del = (key, options, callback) ->
  if (typeof options == 'function')
    callback = options

  if (typeof callback != 'function')
    throw new Error('del() requires a callback argument')

  if (err = this._checkKeyValue(key, 'key', this._isBuffer))
    return callback(err)

  #if (!this._isBuffer(key))
  #  key = String(key)

  if (typeof options != 'object')
    options = {}

  if (typeof this._del == 'function')
    return this._del(key, options, callback)

  process.nextTick(callback)

#-------------------------------------------------------------------------------
# given a mongoDB DB() object, return a 
#-------------------------------------------------------------------------------
module.exports = (mdb) ->

    unless mdb?
        throw Error "database argument was null"

    unless mdb instanceof mongodb.Db
        throw Error "database argument not instance of mongodb.Db"

    #---------------------------------------------------------------------------
    class MongoLeveldown extends AbstractLeveldown.AbstractLevelDOWN

        #-----------------------------------------------------------------------
        @destroy: (location, callback) ->
            console.log "MongoLeveldown::destroy(#{location})" if Debug
            mdb.dropCollection location, (err) ->
                return callback(err) if err
                return callback()

        #-----------------------------------------------------------------------
        @repair: (location, callback) ->
            console.log "MongoLeveldown::repair(#{location})" if Debug
            process.nextTick -> callback()

        #-----------------------------------------------------------------------
        constructor: (location, options) ->
            unless @ instanceof MongoLeveldown
                return new MongoLeveldown location, options

            console.log "new MongoLeveldown(#{location}, #{JS options})" if Debug

            # superclass set's @location to first argument
            super location, options

        #-----------------------------------------------------------------------
        _open: (options, callback) ->
            console.log "MongoLeveldown._open(#{@location}, #{JS options})" if Debug

            mdbOptions =
                w:      1
                fsync:  true

            if options.createIfMissing is false
                mdbOptions.strict = true

            mdb.collection @location, mdbOptions, (err, coll) =>
                return callback err if err?

                if options.errorIfExists
                    return callback Error "collection already exists: #{@location}"

                @coll = coll

                indexOpts = 
                    w:          1
                    fsync:      true
                    unique:     true
                    dropDups:   true
                    name:       "level-db-key"

                coll.ensureIndex {key:1}, indexOpts, (err) ->
                    return callback err if err?
                    callback()

        #-----------------------------------------------------------------------
        _close: (callback) ->
            console.log "MongoLeveldown._close(#{@location})" if Debug

            process.nextTick -> callback()
        
        #-----------------------------------------------------------------------
        _get: (key, options, callback) ->
            options ?= {}
            options.asBuffer = true unless options.asBuffer?

            console.log "MongoLeveldown._get(#{@location}, #{JS key}, #{JS options})" if Debug

            mongoDoc = toMongoDoc {key}

            @coll.findOne mongoDoc, (err, doc) =>
                return callback err if err?

                console.log "_get(#{key}): #{JSON.stringify doc}" if Debug
                unless doc?
                    return callback Error "NotFound"

                {val} = fromMongoDoc doc, {valAsBuffer: options.asBuffer}

                return callback null, val
        
        #-----------------------------------------------------------------------
        _put: (key, val, options, callback) ->
            console.log "MongoLeveldown._put(#{@location}, #{JS key}, #{JS val}, #{JS options})" if Debug

            mdbOptions = w: 1

            mdbOptions.fsync = true if options.sync

            mongoDoc = toMongoDoc {key, val}
            keyDoc   = toMongoDoc {key}

            console.log "_put(#{key}): #{JSON.stringify mongoDoc}" if Debug

            @coll.remove keyDoc, mdbOptions, (err) =>
                return callback err if err?

                @coll.insert mongoDoc, mdbOptions, (err) ->
                    return callback error if err?
                    callback()
        
        #-----------------------------------------------------------------------
        _del: (key, options, callback) ->
            console.log "MongoLeveldown._del(#{@location}, #{JS key}, #{JS options})" if Debug

            mdbOptions = w: 1

            mdbOptions.fsync = true if options.sync

            mongoDoc = toMongoDoc {key}

            @coll.remove mongoDoc, mdbOptions, (err) ->
                return callback err if err?
                callback()
        
        #-----------------------------------------------------------------------
        _batch: (array, options, callback) ->
            console.log "MongoLeveldown._batch(#{@location}, #{JS options})" if Debug
            console.log "#{JL array}" if Debug

            process = (item, callback) =>
                {type, key, value} = item

                if type is "del"
                    @del key, options, callback
                else if type is "put"
                    @put key, value, options, callback

            async.each array, process, (err) ->
                return callback err if err?
                callback()
        
        #-----------------------------------------------------------------------
        _approximateSize: (start, end, callback) ->
            console.log "MongoLeveldown._approximateSize(#{@location})" if Debug
            return 0

        #-----------------------------------------------------------------------
        _iterator: (options) ->
            return new MongoLeveldownIterator @, options

        #-----------------------------------------------------------------------
        _find4iterator: (options, callback) ->
            fields = {}

            delete options.end if options.end is ""
            
            if options.keys
                fields.key     = 1 
                fields.keyType = 1

            if options.values
                fields.val     = 1 
                fields.valType = 1 

            query = {}

            if options.exclusiveStart
                gtKey = "$gt"
                ltKey = "$lt"
            else
                gtKey = "$gte"
                ltKey = "$lte"

            options.start = (toMongoDoc {key: options.start}).key if options.start
            options.end   = (toMongoDoc {key: options.end}).key   if options.end

            if !options.reverse
                if options.start? and options.end?
                    query.$and = [
                        {key: {}}
                        {key: $lte: options.end}
                    ]
                    query.$and[0].key[gtKey] = options.start

                else if options.start?
                    query.key = {}
                    query.key[gtKey] = options.start

                else if options.end?
                    query.key = $lte: options.end

            else
                if options.start? and options.end?
                    query.$and = [
                        {key: {}}
                        {key: $gte: options.end}
                    ]
                    query.$and[0].key[ltKey] = options.start

                else if options.start?
                    query.key = {}
                    query.key[ltKey] = options.start

                else if options.end?
                    query.key = $gte: options.end

            if options.reverse
                sortOrder = -1
            else
                sortOrder =  1

            findOptions =
                sort: {key: sortOrder}

            if options.limit isnt -1
                findOptions.limit = options.limit

            console.log "coll.find(#{JS query}, #{JS fields}, #{JS findOptions}), " # if Debug
            return @coll.find(query, fields, findOptions)

#-------------------------------------------------------------------------------
class MongoLeveldownIterator extends AbstractLeveldown.AbstractIterator

    constructor: (db, options) ->

        # superclass set's @db to first argument
        super db

        # abstract iterator doesn't cache options, so we do
        # options include the undocumented `exclusiveStart`
        # property, in support of the other undocumented properties
        # `lt`, `gt`, etc.  `exclusiveStart` means "skip the first
        # iterated item if it matches the start key passed in"
        #     - skip start if found; for lt/gt/etc undocumented opts

        @_options =
            start          : options.start
            end            : options.end
            reverse        : if options.reverse?        then !!options.reverse        else false
            keys           : if options.keys?           then !!options.keys           else true
            values         : if options.values?         then !!options.values         else true
            limit          : if options.limit?          then   options.limit          else -1
            keyAsBuffer    : if options.keyAsBuffer?    then !!options.keyAsBuffer    else true
            valueAsBuffer  : if options.valueAsBuffer?  then !!options.valueAsBuffer  else true
            exclusiveStart : if options.exclusiveStart? then !!options.exclusiveStart else false

        @_error  = null
        @_cursor = @db._find4iterator @_options, (err) =>
            @_error = err

    _next: (callback) ->
        if @_error?
            @_cursor.close()
            # @_ended = true # for superclass
            # console.log "calling next() cb: error"
            return callback err

        if @_cursor.isClosed()
            # @_ended = true # for superclass
            # console.log "calling next() cb: closed"
            return callback()

        if @_ended
            @_cursor.close()
            # console.log "calling next() cb: ended"
            return callback()

        @_cursor.nextObject (err, item) =>
            # console.log "next() -> err: #{err}, item: #{JS item}"

            if err?
                @_cursor.close()
                # @_ended = true # for superclass
                # console.log "calling next() cb: error"
                return callback err

            if !item?
                @_cursor.close()
                # @_ended = true # for superclass
                # console.log "calling next() cb: error"
                return callback()

            {key, val} = fromMongoDoc item,
                keyAsBuffer: @_options.keyAsBuffer
                valAsBuffer: @_options.valueAsBuffer
            
            console.log "next() calling cb: #{JS {key, val}}" if Debug
            console.log "   key: ", key if Debug
            console.log "   val: ", val if Debug

            callback null, key, val

        return

#-------------------------------------------------------------------------------
toMongoDoc = ({key, val}) ->
    doc = {}

    if key?
        if Buffer.isBuffer key
            doc.keyType = "b"
            doc.key     = key.toString "hex"
        else if typeof key is "string"
            doc.keyType = "s"
            doc.key     = (new Buffer(key)).toString("hex")
        else 
            doc.keyType = "j"
            doc.key     = (new Buffer(JSON.stringify(key))).toString("hex")

    if val?
        if Buffer.isBuffer val
            doc.valType = "b"
            doc.val     = val.toString "hex"
        else if typeof val is "string"
            doc.valType = "s"
            doc.val     = (new Buffer(val)).toString("hex")
        else 
            doc.valType = "j"
            doc.val     = (new Buffer(JSON.stringify(val))).toString("hex")

    return doc

#-------------------------------------------------------------------------------
fromMongoDoc = (doc, {keyAsBuffer, valAsBuffer}) ->
    console.log "fromMongoDoc(#{JS doc}, {keyasBuffer:#{keyAsBuffer}, valAsBuffer:#{valAsBuffer})" if Debug
    key = null
    val = null

    if doc.keyType? and doc.key?
        buffer = new Buffer(doc.key, "hex")
        if doc.keyType   is "b"  or keyAsBuffer
            key = buffer
        else if doc.keyType is "s"
            key = buffer.toString()
        else if doc.keyType is "j"
            key = JSON.parse(buffer.toString())
        else 
            throw Error "invalid keyType: #{doc.keyType}"

    if doc.valType? and doc.val?
        buffer = new Buffer(doc.val, "hex")
        if doc.valType   is "b" or valAsBuffer
            val = buffer
        else if doc.valType is "s"
            val = buffer.toString()
        else if doc.valType is "j"
            val = JSON.parse(buffer.toString())
        else 
            throw Error "invalid valType: #{doc.valType}"

    return {key, val}

#-------------------------------------------------------------------------------
JS = (object) -> JSON.stringify object
JL = (object) -> JSON.stringify object, null, 4

#-------------------------------------------------------------------------------
# Copyright 2013 Patrick Mueller
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------
