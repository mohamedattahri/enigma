# Go-Enigma [![Build Status](https://travis-ci.org/mohamedattahri/go-enigma.svg?branch=master)](https://travis-ci.org/mohamedattahri/go-enigma) [![GoDoc](https://godoc.org/github.com/mohamedattahri/go-enigma?status.svg)](https://godoc.org/github.com/mohamedattahri/go-enigma)

[Enigma.io](http://enigma.io) lets you quickly search and analyze billions of public records published by governments, companies and organizations.

Go-Enigma is a simple go client for the [enigma.io](https://app.enigma.io/api) API.

## Documentation

Full documentation and examples are available in the [godoc package index](http://godoc.org/github.com/mohamedattahri/go-enigma) [![GoDoc](https://godoc.org/github.com/mohamedattahri/go-enigma?status.svg)](https://godoc.org/github.com/mohamedattahri/enigma)

## Examples

### Client

````go
package main

import (
	enigma "github.com/mohamedattahri/go-enigma"
)

func main() {
	client := enigma.NewClient("some_api_key")
}

````

### Metadata

#### Parent

````go
response, err := client.Meta().Parent("us.gov.whitehouse")
if err != nil {
	fmt.Println(err)
	return
}
fmt.Println(response.Info.ChildrenTablesTotal)
````

#### Table

````go
response, err := client.Meta().Table("us.gov.whitehouse.visitor-list")
if err != nil {
	fmt.Println(err)
	return
}
fmt.Println(response.Result.DbBoundaryLabel)
````

### Data

````go
response, err := client.Data("us.gov.whitehouse.visitor-list").Select("namefull", "appt_made_date").Sort("namefirst", enigma.Desc).Results()
if err != nil {
	fmt.Println(err)
	return
}
fmt.Println(string(response.Result))
````

### Stats

````go
response, err := client.Stats("us.gov.whitehouse.visitor-list", "total_people").Operation(enigma.Sum).Results()
if err != nil {
	fmt.Println(err)
	return
}

var obj map[string]string
json.Unmarshal(response.Result, &obj)
fmt.Println(obj["sum"])
````

### Export

````go
url, err := client.Export("us.gov.whitehouse.visitor-list").FileURL(nil)
if err != nil {
	fmt.Println(err)
	return
}
fmt.Println(url)
````

## TODO:
More tests.
