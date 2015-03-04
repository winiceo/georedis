Go implementation of geo searching using Redis
---

[![Build Status](https://travis-ci.org/tapglue/georedis.svg)](https://travis-ci.org/tapglue/georedis)
[![Coverage Status](https://coveralls.io/repos/tapglue/georedis/badge.svg?branch=master)](https://coveralls.io/r/tapglue/georedis?branch=master)
[![GoDoc](https://godoc.org/github.com/tapglue/georedis?status.svg)](https://godoc.org/github.com/tapglue/georedis)

This is a Go implementation of the geo proximity library
ported from the nodejs library [node-geo-proximity](https://github.com/arjunmehta/node-geo-proximity)

Dependencis
===
goredis depends on:

- geo hashing package [geohash](https://github.com/tapglue/geohash)
- go-redis package [gopkg.in/redis.v2](https://gopkg.in/redis.v2)

License
===
georedis is licensed under MIT license.
Please see [LICENSE.md](LICENSE.md) for the full license.
