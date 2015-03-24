/**
 * This code is licensed under MIT license.
 * Please see LICENSE.md file for full license.
 */

// Package georedis implements geo location functionality for go using redis
package georedis

import (
	"fmt"
	"math"
	"sort"

	"github.com/tapglue/geohash"

	"gopkg.in/redis.v2"
)

type (
	// GeoKey provides support for encoding a location with a label and coordinates
	GeoKey struct {
		Lat   float64
		Lon   float64
		Label string
	}

	geoRange struct {
		Lower float64
		Upper float64
	}
)

var (
	rangeIndex = map[uint8]float64{
		0:  0.6,      //52
		1:  1,        //50
		2:  2.19,     //48
		3:  4.57,     //46
		4:  9.34,     //44
		5:  14.4,     //42
		6:  33.18,    //40
		7:  62.1,     //38
		8:  128.55,   //36
		9:  252.9,    //34
		10: 510.02,   //32
		11: 1015.8,   //30
		12: 2236.5,   //28
		13: 3866.9,   //26
		14: 8749.7,   //24
		15: 15664,    //22
		16: 33163.5,  //20
		17: 72226.3,  //18
		18: 150350,   //16
		19: 306600,   //14
		20: 474640,   //12
		21: 1099600,  //10
		22: 2349600,  //8
		23: 4849600,  //6
		24: 10018863, //4
	}
	rangeIndexLen = uint8(len(rangeIndex))
)

func rangeDepth(radius float64) uint8 {
	var i uint8
	for i = 0; i < rangeIndexLen-1; i++ {
		if radius-rangeIndex[i] < rangeIndex[i+1]-radius {
			return 52 - (i * 2)
		}
	}

	return 2
}

// AddCoordinates adds coordinates to the set
func AddCoordinates(client *redis.Client, bucketName string, bitDepth uint8, coordinates ...GeoKey) (int64, error) {
	encodedCoordinates := make([]redis.Z, len(coordinates))

	for key, value := range coordinates {
		encodedCoordinate := geohash.EncodeInt(
			value.Lat,
			value.Lon,
			bitDepth,
		)
		encodedCoordinates[key] = redis.Z{
			Score:  float64(encodedCoordinate),
			Member: value.Label,
		}
	}

	return client.ZAdd(bucketName, encodedCoordinates...).Result()
}

// RemoveCoordinatesByKeys removes coordinates from the set
func RemoveCoordinatesByKeys(client *redis.Client, bucketName string, coordinatesKeys ...string) (int64, error) {
	return client.ZRem(bucketName, coordinatesKeys...).Result()
}

// SearchByRadius returns all keys which are in a certain range from the provided lat & lon coordinates
func SearchByRadius(client *redis.Client, bucketName string, lat, lon, radius float64, bitDepth uint8) ([]string, error) {
	radiusBitDepth := rangeDepth(radius)
	ranges, err := getQueryRangesFromBitDepth(lat, lon, radiusBitDepth, bitDepth)
	if err != nil {
		return []string{}, err
	}

	return queryByRanges(client, bucketName, ranges, lat, lon, bitDepth)
}

// SearchByRadiusWithLimit returns all keys which are in a certain range from the provided lat & lon coordinates and returns only the first "limit" items
func SearchByRadiusWithLimit(client *redis.Client, bucketName string, lat, lon, radius float64, bitDepth uint8, limit int) ([]string, error) {
	radiusBitDepth := rangeDepth(radius)
	ranges, err := getQueryRangesFromBitDepth(lat, lon, radiusBitDepth, bitDepth)
	if err != nil {
		return []string{}, err
	}

	return queryByRangesWithLimit(client, bucketName, ranges, lat, lon, bitDepth, limit)
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func getQueryRangesFromBitDepth(lat, lon float64, radiusBitDepth, bitDepth uint8) ([]geoRange, error) {
	bitDiff := bitDepth - radiusBitDepth
	if bitDiff < 0 {
		return []geoRange{}, fmt.Errorf("bitDepth must be high enough to calculate range within radius")
	}

	hash := geohash.EncodeInt(lat, lon, radiusBitDepth)
	neighbors := geohash.EncodeNeighborsInt(hash, radiusBitDepth)

	neighbors = append(neighbors, hash)
	sort.Sort(uint64Slice(neighbors))

	if radiusBitDepth <= 4 {
		neighbors = uniqueInSlice(neighbors)
	}

	ranges := []geoRange{}

	for i := 0; i < len(neighbors); i++ {
		lowerRange := float64(neighbors[i])
		upperRange := lowerRange + 1

		for len(neighbors) > i+1 && float64(neighbors[i+1]) == upperRange {
			neighbors = neighbors[1:]
			upperRange = float64(neighbors[i] + 1)
		}

		ranges = append(ranges, geoRange{Lower: lowerRange, Upper: upperRange})
	}

	for key := range ranges {
		ranges[key].Lower = leftShift(ranges[key].Lower, bitDiff)
		ranges[key].Upper = leftShift(ranges[key].Upper, bitDiff)
	}

	return ranges, nil
}

func queryByRanges(client *redis.Client, bucketName string, ranges []geoRange, lat, lon float64, depth uint8) ([]string, error) {
	var results []redis.Z

	for key := range ranges {
		res, err := client.ZRangeByScoreWithScores(
			bucketName,
			redis.ZRangeByScore{
				Min: fmt.Sprintf("%f", ranges[key].Lower),
				Max: fmt.Sprintf("%f", ranges[key].Upper),
			},
		).Result()
		if err == nil {
			results = append(results, res...)
		}
	}

	return sortResults(lat, lon, depth, results, -1), nil
}

func queryByRangesWithLimit(client *redis.Client, bucketName string, ranges []geoRange, lat, lon float64, depth uint8, limit int) ([]string, error) {
	var results []redis.Z

	limit64 := int64(limit)

	for key := range ranges {
		res, err := client.ZRangeByScoreWithScores(
			bucketName,
			redis.ZRangeByScore{
				Min:   fmt.Sprintf("%f", ranges[key].Lower),
				Max:   fmt.Sprintf("%f", ranges[key].Upper),
				Count: limit64,
			},
		).Result()
		if err == nil {
			results = append(results, res...)
		}
	}

	return sortResults(lat, lon, depth, results, limit), nil
}

func uniqueInSlice(slice []uint64) []uint64 {
	result := []uint64{}
	used := make(map[uint64]byte, len(slice))

	for key := range slice {
		if used[slice[key]] == 1 {
			continue
		}
		result = append(result, slice[key])
		used[slice[key]] = 1
	}

	return result
}

func leftShift(x float64, shift uint8) float64 {
	return x * math.Pow(2, float64(shift))
}

type (
	labelWithDistance struct {
		Label    string
		Distance float64
	}
	labelsWithDistance []labelWithDistance
)

func (l labelsWithDistance) Len() int           { return len(l) }
func (l labelsWithDistance) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l labelsWithDistance) Less(i, j int) bool { return l[i].Distance < l[j].Distance }

func sortResults(lat, lon float64, depth uint8, points []redis.Z, limit int) []string {
	if limit == -1 {
		limit = len(points)
	} else if limit > len(points) {
		limit = len(points)
	}

	results := make([]labelWithDistance, limit)
	for idx := range points {
		pointLat, pointLon, _, _ := geohash.DecodeInt(uint64(points[idx].Score), depth)
		results[idx] = labelWithDistance{
			Label:    points[idx].Member,
			Distance: geohash.DistanceBetweenPoints(lat, lon, pointLat, pointLon),
		}
	}

	sort.Sort(labelsWithDistance(results))

	asString := make([]string, limit)
	for i := 0; i < limit; i++ {
		asString[i] = results[i].Label
	}

	return asString
}
