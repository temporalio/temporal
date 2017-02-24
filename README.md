cadence 
==============
[Cadence](https://eng.uber.com/) is awesome!

Developing
----------

Prerequisite:
* Make certain that `thrift` is in your path. (OSX: `brew install thrift`) 
* `thrift-gen` is needed (`go get github.com/uber/tchannel-go/thrift/thrift-gen`)
* `cassandra` 3.9 is needed to run tests (OSX: `brew install cassandra`)

After starting cassandra, run `make` to build

Contributing
------------
We'd love your help in making Cadence great. If you need new API(s) to be added to our thrift files, open an issue and we will respond as fast as we can. If you want to propose new feature(s) along with the appropriate APIs yourself, open a pull request to start a discussion and we will merge it after review.

**Note:** All contributors also need to fill out the [Uber Contributor License Agreement](http://t.uber.com/cla) before we can merge in any of your changes

Documentation
--------------
Interested in learning more about Cadence? TODO:

License
-------
MIT License, please see [LICENSE](https://github.com/uber/cadence/blob/master/LICENSE) for details.
