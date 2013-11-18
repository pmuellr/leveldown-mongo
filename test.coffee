# Licensed under the Apache License. See footer for details.

DB_URL = process.env.DB_URL || "mongodb://localhost:27017/leveldown-mongo-tests"

Math.randomOriginal = Math.random
Math.random         = -> Math.floor(100 * Math.randomOriginal())

path = require "path"

test            = require "tape"
testCommon      = require "abstract-leveldown/testCommon"
mongodb         = require "mongodb"

MongoLevelDown  = require "./"

# hack location et al from testCommon - names are too long

testCommon.locationOriginal     = testCommon.location
testCommon.lastLocationOriginal = testCommon.lastLocation

testCommon.location = -> 
    name = path.basename testCommon.locationOriginal()
    name.replace("_leveldown_test_db_", "test")

testCommon.lastLocation = -> 
    name = path.basename testCommon.lastLocationOriginal()
    name.replace("_leveldown_test_db_", "test")

testCommon.setUp = (t) -> 
    testCommon.cleanup (err) ->
        t.notOk err, "cleanup returned an error: #{err}"
        t.end()

testCommon.tearDown = testCommon.setUp

testCommon.cleanup  = (callback) ->
    mongodb.MongoClient.connect DB_URL, (err, mdb) ->
        mdb.dropDatabase callback if callback?

# original collect entries converts numeric-ish keys to numbers!  stop it!
testCommon.collectEntries = (iterator, callback) ->
    data = []
    next = ->
        iterator.next (err, key, value) ->
            return callback(err) if err

            if (!arguments.length)
                return iterator.end (err) ->
                    callback(err, data)

            data.push { key, value }
            process.nextTick(next)

    next()

mongodb.MongoClient.connect DB_URL, (err, mdb) ->
    runTest mdb, "open-test", "args"
    runTest mdb, "open-test", "open"
    runTest mdb, "del-test"
    runTest mdb, "get-test"
    runTest mdb, "put-test"
    runTest mdb, "batch-test"
    runTest mdb, "close-test", "close"

    runTest mdb, "iterator-test"
    # runTest mdb, "chained-batch-test"
    # runTest mdb, "ranges-test"

    setTimeout (-> process.exit 0), 10 * 1000

#-------------------------------------------------------------------------------
runTest = (mdb, name, fn="all") ->
    mod = require "abstract-leveldown/abstract/#{name}"
    db  = new MongoLevelDown mdb

    mod[fn] db, test, testCommon

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
