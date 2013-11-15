# Licensed under the Apache License. See footer for details.

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
            # console.log "MongoLeveldown::destroy(#{location})"
            mdb.dropCollection location, (err) ->
                return callback(err) if err
                return callback()

        #-----------------------------------------------------------------------
        @repair: (location, callback) ->
            # console.log "MongoLeveldown::repair(#{location})"
            process.nextTick -> callback()

        #-----------------------------------------------------------------------
        constructor: (location, options) ->
            unless @ instanceof MongoLeveldown
                return new MongoLeveldown location, options

            # console.log "new MongoLeveldown(#{location}, #{JS options})"

            # superclass set's @location to first argument
            super location, options

        #-----------------------------------------------------------------------
        _open: (options, callback) ->
            # console.log "MongoLeveldown._open(#{@location}, #{JS options})"

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

                return callback()

        #-----------------------------------------------------------------------
        _close: (callback) ->
            # console.log "MongoLeveldown._close(#{@location})"

            process.nextTick -> callback()
        
        #-----------------------------------------------------------------------
        _get: (key, options, callback) ->
            options ?= {}
            options.asBuffer = true unless options.asBuffer?

            # console.log "MongoLeveldown._get(#{@location}, #{JS key}, #{JS options})"

            @coll.findOne {key}, (err, doc) =>
                return callback err if err?

                # console.log "_get(#{key}): #{JSON.stringify doc}"
                unless doc?
                    return callback Error "NotFound"

                val = doc.val
                if options.asBuffer
                    val = new buffer.Buffer(val, "utf8")

                return callback null, val
        
        #-----------------------------------------------------------------------
        _put: (key, val, options, callback) ->
            # console.log "MongoLeveldown._put(#{@location}, #{JS key}, #{JS val}, #{JS options})"

            mdbOptions = w: 1

            mdbOptions.fsync = true if options.sync

            doc = {key, val}
            doc.val = val.toString "utf8" if isBuffer val

            # console.log "_put(#{key}): #{JSON.stringify doc}"

            @coll.remove {key}, mdbOptions, (err) =>
                return callback err if err?

                @coll.insert doc, mdbOptions, (err) ->
                    return callback error if err?
                    callback()
        
        #-----------------------------------------------------------------------
        _del: (key, options, callback) ->
            # console.log "MongoLeveldown._del(#{@location}, #{JS key}, #{JS options})"

            mdbOptions = w: 1

            mdbOptions.fsync = true if options.sync

            @coll.remove {key}, mdbOptions, (err) ->
                return callback err if err?
                callback()
        
        #-----------------------------------------------------------------------
        _batch: (array, options, callback) ->
            # console.log "MongoLeveldown._batch(#{@location}, #{JS options})"
            # console.log "#{JL array}"

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
            # console.log "MongoLeveldown._approximateSize(#{@location})"
            return 0

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
