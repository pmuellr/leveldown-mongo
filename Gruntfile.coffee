# Licensed under the Apache License. See footer for details.

#-------------------------------------------------------------------------------

fs            = require "fs"
path          = require "path"
zlib          = require "zlib"

require "shelljs/global"
_ = require "underscore"

__basename = path.basename __filename

mkdir "-p", "tmp"

#-------------------------------------------------------------------------------

grunt = null

Config =

    watch:
        Gruntfile:
            files: __basename
            tasks: "gruntfile-changed"
        source:
            files: [ "lib-src/**/*", "test.coffee" ]
            tasks: "build-n-test"
            options:
                atBegin:    true
                interrupt:  true

    clean: [
        "lib"
        "node_modules"
        "tmp"
    ]

#-------------------------------------------------------------------------------
module.exports = (Grunt) ->
    grunt = Grunt

    grunt.initConfig Config

    grunt.registerTask "default", ["help"]

    grunt.registerTask "help", "print help", ->
        exec "grunt --help"

    grunt.loadNpmTasks "grunt-contrib-watch"

    grunt.registerTask "build", "run a build", ->
        build @

    grunt.registerTask "test", "run the tests", ->
        coffee "test.coffee"

    grunt.registerTask "clean", "remove transient files", ->
        clean @

    grunt.registerTask "----------------", "remaining tasks are internal", ->

    grunt.registerTask "build-n-test", "run a build, then the tests", ->
        grunt.task.run ["build", "test"]

    grunt.registerTask "gruntfile-changed", "exit when the Gruntfile changes", ->
        grunt.log.write "Gruntfile changed, maybe you wanna exit and restart?"

#-------------------------------------------------------------------------------
build = (task) ->
    log "building code in lib"

    cleanDir "lib"

    coffeec "--output lib lib-src/*.coffee"

#-------------------------------------------------------------------------------
test = (task) ->
    coffee "test-leveldown.coffee"


#-------------------------------------------------------------------------------
clean = ->
    for dir in Config.clean
        if test "-d", dir
            rm "-rf", dir

#-------------------------------------------------------------------------------
cleanDir = (dirs...) ->
    for dir in dirs
        mkdir "-p",  dir
        rm "-rf", "#{dir}/*"

#-------------------------------------------------------------------------------

coffee  = (parms) ->  exec "node_modules/.bin/coffee #{parms}"
coffeec = (parms) ->  coffee "--bare --compile #{parms}"
tap     = (parms) ->  exec "node_modules/.bin/tap #{parms}"

#-------------------------------------------------------------------------------
log = (message) ->
    grunt.log.write "#{message}\n"

#-------------------------------------------------------------------------------
logError = (message) ->
    grunt.fail.fatal "#{message}\n"

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
