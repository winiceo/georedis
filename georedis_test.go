/**
 * This code is licensed under MIT license.
 * Please see LICENSE.md file for full license.
 */

package georedis_test

import (
	"flag"
	"testing"

	. "github.com/tapglue/georedis"

	"gopkg.in/redis.v2"
)

const (
	zSetNameOne  = "test:add:one"
	zSetNameMany = "test:add:many"
	zSetPeople   = "test:search:people"
	zSetCities   = "test:search:cities"

	bitDepth       = 52
	radiusBitDepth = 48
)

var (
	client *redis.Client

	oneCoordinate   = GeoKey{Lat: 1, Lon: 1, Label: "demo"}
	manyCoordinates = []GeoKey{
		{Lat: 1, Lon: 1, Label: "demo1"},
		{Lat: 1, Lon: 1, Label: "demo2"},
		{Lat: 2, Lon: 1, Label: "demo3"},
		{Lat: 2, Lon: 2, Label: "demo4"},
	}
)

func init() {
	address := flag.String("address", "127.0.0.1:6379", "Redis address")
	password := flag.String("password", "", "Redis password")
	database := flag.Int64("database", 0, "Redis database")
	flag.Parse()

	options := &redis.Options{
		Addr:     *address,
		Password: *password,
		DB:       *database,
		PoolSize: 2,
	}

	client = redis.NewTCPClient(options)
}

func TestAddCoordinatesOne(t *testing.T) {
	added, err := AddCoordinates(client, zSetNameOne, bitDepth, oneCoordinate)
	if err != nil {
		t.Logf("error encountered %q\n", err)
		t.Fail()
	}
	if added != 1 {
		t.Logf("expected to add: %d added: %d\n", 1, added)
		t.Fail()
	}
}

func TestAddCoordinatesMany(t *testing.T) {
	added, err := AddCoordinates(client, zSetNameMany, bitDepth, manyCoordinates...)
	if err != nil {
		t.Logf("error encountered %q\n", err)
		t.Fail()
	}
	if added != int64(len(manyCoordinates)) {
		t.Logf("expected to add: %d added: %d\n", len(manyCoordinates), added)
		t.Fail()
	}
}

func TestRemoveCoordinatesByKeysOne(t *testing.T) {
	removed, err := RemoveCoordinatesByKeys(client, zSetNameOne, oneCoordinate.Label)
	if err != nil {
		t.Logf("error encountered %q\n", err)
		t.Fail()
	}
	if removed != 1 {
		t.Logf("expected to remove: %d removed: %d", 1, removed)
		t.Fail()
	}
}

func TestRemoveCoordinatesByKeysMany(t *testing.T) {
	keys := make([]string, len(manyCoordinates))
	for idx, val := range manyCoordinates {
		keys[idx] = val.Label
	}
	removed, err := RemoveCoordinatesByKeys(client, zSetNameMany, keys...)
	if err != nil {
		t.Logf("error encountered %q\n", err)
		t.Fail()
	}
	if removed != int64(len(manyCoordinates)) {
		t.Logf("expected to remove: %d removed: %d", len(manyCoordinates), removed)
		t.Fail()
	}
}

func TestSearchByRadius(t *testing.T) {
	peopleCoordinates := []GeoKey{
		{Lat: 43.6667, Lon: -79.4167, Label: "John"},
		{Lat: 39.9523, Lon: -75.1638, Label: "Shankar"},
		{Lat: 37.4688, Lon: -122.1411, Label: "Cynthia"},
		{Lat: 37.7691, Lon: -122.4449, Label: "Chen"},
	}

	RemoveCoordinatesByKeys(client, zSetPeople, "John", "Shankar", "Cynthia", "Chen")
	AddCoordinates(client, zSetPeople, bitDepth, peopleCoordinates...)

	people, err := SearchByRadius(client, zSetPeople, 39.9523, -75.1638, 5000, bitDepth)
	if err != nil {
		t.Logf("error encountered: %q\n", err)
		t.Fail()
	}
	if len(people) != 1 {
		t.Logf("unexpected number of items retrieved expected: %d got: %d items: %v", 1, len(people), people)
		t.Fail()
	}
	if people[0] != "Shankar" {
		t.Logf("wrong person retrieved expected: %s got: %s", "Shankar", people[0])
		t.Fail()
	}

	placesCoordinates := []GeoKey{
		{Lat: 43.6667, Lon: -79.4167, Label: "Toronto"},
		{Lat: 39.9523, Lon: -75.1638, Label: "Philadelphia"},
		{Lat: 37.4688, Lon: -122.1411, Label: "Palo Alto"},
		{Lat: 37.7691, Lon: -122.4449, Label: "San Francisco"},
		{Lat: 47.5500, Lon: -52.6667, Label: "St. John's"},
	}

	RemoveCoordinatesByKeys(client, zSetCities, "Toronto", "Philadelphia", "Palo Alto", "San Francisco", "St. John's")
	AddCoordinates(client, zSetCities, bitDepth, placesCoordinates...)

	cities, err := SearchByRadius(client, zSetCities, 39.9523, -75.1638, 5000, bitDepth)
	if err != nil {
		t.Logf("error encountered: %q\n", err)
		t.Fail()
	}
	if len(cities) != 1 {
		t.Logf("unexpected number of items retrieved expected: %d got: %d items: %v", 1, len(people), people)
		t.Fail()
	}
	if cities[0] != "Philadelphia" {
		t.Logf("wrong person retrieved expected: %s got: %s", "Philadelphia", people[0])
		t.Fail()
	}
}
