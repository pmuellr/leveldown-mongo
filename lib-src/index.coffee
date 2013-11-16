# Licensed under the Apache License. See footer for details.

Debug = true

buffer = require "buffer"

_       = require "underscore"
mongodb = require "mongodb"

AbstractLeveldown = require "abstract-leveldown"

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

            @coll.findOne {key}, (err, doc) =>
                return callback err if err?

                console.log "_get(#{key}): #{JSON.stringify doc}" if Debug
                unless doc?
                    return callback Error "NotFound"

                val = doc.val
                if options.asBuffer
                    val = new buffer.Buffer(val, "utf8")

                return callback null, val
        
        #-----------------------------------------------------------------------
        _put: (key, val, options, callback) ->
            console.log "MongoLeveldown._put(#{@location}, #{JS key}, #{JS val}, #{JS options})" if Debug

            mdbOptions = w: 1

            mdbOptions.fsync = true if options.sync

            doc = {key, val}
            doc.val = val.toString "utf8" if isBuffer val

            console.log "_put(#{key}): #{JSON.stringify doc}" if Debug

            @coll.remove {key}, mdbOptions, (err) =>
                return callback err if err?

                @coll.insert doc, mdbOptions, (err) ->
                    return callback error if err?
                    callback()
        
        #-----------------------------------------------------------------------
        _del: (key, options, callback) ->
            console.log "MongoLeveldown._del(#{@location}, #{JS key}, #{JS options})" if Debug

            mdbOptions = w: 1

            mdbOptions.fsync = true if options.sync

            @coll.remove {key}, mdbOptions, (err) ->
                return callback err if err?
                callback()
        
        #-----------------------------------------------------------------------
        _batch: (array, options, callback) ->
            console.log "MongoLeveldown._batch(#{@location}, #{JS options})" if Debug
            console.log "#{JL array}" if Debug

            for {type, key, value} in array
                val = value
                doc = {key, val}
                if type is "del"
                    @coll.remove {key}, {w:0}
                else if type is "put"
                    @coll.remove {key}, {w:0}
                    @coll.insert doc, {w:0}

            process.nextTick ->callback()
        
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
            fields.key = 1 if options.keys
            fields.val = 1 if options.values

            if options.limit isnt -1
                fields.array = $slice: options.limit

            query = {}

            if options.exclusiveStart
                gtKey = "$gt"
            else
                gtKey = "$gte"

            if options.start? and options.end?
                query.$and = [
                    {key: {}}
                    {key: $lte: options.end}
                ]
                query.$and[0][key][gtKey] = options.start

            else if options.start?
                query.key = {}
                query.key[gtKey] = options.start
            else if options.end?
                query.key = $lte: options.end

            console.log "coll.find(\n#{JL query},\n#{JL fields})"    

            if options.reverse
                sortOrder = -1
            else
                sortOrder =  1

            return @coll.find(query, fields).sort(key:sortOrder)

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

            {key, val} = item
            
            key = new buffer.Buffer key, "utf8" if @_options.keyAsBuffer
            val = new buffer.Buffer val, "utf8" if @_options.valueAsBuffer

            console.log "next() calling cb: #{JS {key, val}}"
            callback null, key, val

#-------------------------------------------------------------------------------
JS = (object) -> JSON.stringify object
JL = (object) -> JSON.stringify object, null, 4

#-------------------------------------------------------------------------------
isBuffer = (object) ->
    return buffer.Buffer.isBuffer object

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
