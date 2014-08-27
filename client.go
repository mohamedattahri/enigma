// Package enigma provides developers with a Go client for the Enigma.io API.
//
// The Enigma API allows users to download datasets, query metadata, or perform server side operations on tables in Enigma.
// All calls to the API are made through a RESTful protocol and require an API key.
// The Enigma API is served over HTTPS. To ensure data privacy, unencrypted HTTP is not supported.
package enigma

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	root    = "https://api.enigma.io" //<version>/<endpoint>/<api key>/<datapath>/<parameters>
	version = "v2"
)

type endpoint string

const (
	meta   endpoint = "meta"
	data   endpoint = "data"
	stats  endpoint = "stats"
	export endpoint = "export"
)

// Conjunction represents the logical link between multiple search or where parameters.
type Conjunction string

const (
	Or  Conjunction = "or"
	And Conjunction = "and"
)

// SortDirection represents the direction in which a selected column or calculation result
// should be sorted.
type SortDirection string

const (
	Asc  SortDirection = "+"
	Desc SortDirection = "-"
)

// Operation represents a calculation that a stats request can perform on a selected column.
type Operation string

const (
	Sum       Operation = "sum"
	Avg       Operation = "avg"
	StdDev    Operation = "stddev"
	Variance  Operation = "variance"
	Max       Operation = "max"
	Min       Operation = "min"
	Frequency Operation = "frequency"
)

type query struct {
	baseUri  string
	datapath string
	params   url.Values
}

func doQuery(baseUri, datapath string, params url.Values, response interface{}) (err error) {
	uri := baseUri + "/" + datapath
	if len(params) > 0 {
		uri += "?" + params.Encode()
	}

	resp, err := http.Get(uri)
	if err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// API error handling
	if resp.StatusCode != 200 {
		var e map[string]interface{}
		if json.Unmarshal(body, &e) != nil {
			return errors.New(resp.Status)
		} else {
			return errors.New(e["info"].(map[string]interface{})["additional"].(string))
		}
	}

	// Parsing the response into the provided response struct.
	if err = json.Unmarshal(body, &response); err != nil {
		return
	}

	return
}

type MetaParentNodeResponse struct {
	DataPath string `json:"data_path"`
	Result   struct {
		Path []struct {
			Level       string `json:"level"`
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"path"`
		ImmediateNodes []struct {
			Datapath    string `json:"datapath"`
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"immediate_nodes"`
		ChildrenTables []struct {
			Datapath         string `json:"datapath"`
			Label            string `json:"label"`
			Description      string `json:"description"`
			DbBoundaryLabel  string `json:"db_boundary_label"`
			DbBoundaryTables string `json:"db_boundary_tables"`
		} `json:"children_tables"`
	} `json:"result"`
	Info struct {
		ResultType          string `json:"result_type"`
		ChildrenTablesLimit int    `json:"children_tables_limit"`
		ChildrenTablesTotal int    `json:"children_tables_total"`
		CurrentPage         int    `json:"current_page"`
		TotalPages          int    `json:"total_pages"`
	} `json:"info"`
}

type MetaTableNodeResponse struct {
	// DataPath string `json:"datapath"`
	Result struct {
		Path []struct {
			Level       string `json:"level"`
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"path"`
		Columns []struct {
			ID          string `json:"id"`
			Label       string `json:"label"`
			Description string `json:"description"`
			Type        string `json:"type"`
			Index       int    `json:"index"`
		} `json:"columns"`
		DbBoundaryDatapath string `json:"db_boundary_datapath"`
		DbBoundaryLabel    string `json:"db_boundary_label"`
		DbBoundaryTables   []struct {
			Datapath string `json:"datapath"`
			Label    string `json:"label"`
		} `json:"db_boundary_tables"`
		AncestorDatapaths []string `json:"ancestor_datapaths"`
		Documents         []struct {
			Url   string `json:"url"`
			Title string `json:"title"`
			Type  string `json:"type"`
		} `json:"documents"`
		Metadata []struct {
			Value string `json:"value"`
			Label string `json:"label"`
		} `json:"metadata"`
	} `json:"result"`
	Info struct {
		ResultType string `json:"result_type"`
	} `json:"info"`
}

type metaQuery query

func (q *metaQuery) Parent(datapath string) (response *MetaParentNodeResponse, err error) {
	err = doQuery(q.baseUri, datapath, q.params, &response)
	return
}

// Results or error returned by the server.
func (q *metaQuery) Table(datapath string) (response *MetaTableNodeResponse, err error) {
	err = doQuery(q.baseUri, datapath, q.params, &response)
	return
}

// StatsResponse attributes
type StatsResponse struct {
	DataPath string          `json:"data_path"`
	Result   json.RawMessage `json:"result"`
	Info     struct {
		Column       interface{} `json:"column"`
		Operations   []Operation `json:"operations"`
		RowsLimit    int         `json:"rows_limit"`
		CurrentPage  int         `json:"current_page"`
		TotalPages   int         `json:"total_pages"`
		TotalResults int         `json:"total_results"`
	} `json:"info"`
}

// Table datapaths can be queried by column for statistics on the data that it contains.
// Like data queries, stats queries may be filtered, sorted and paginated using the provided URL parameters.
type statsQuery query

// Limit the number of frequency, compound sum, or compound average results returned (max. 500).
// Defaults to 500.
func (q *statsQuery) Limit(limit int) *statsQuery {
	q.params.Add("limit", strconv.Itoa(limit))
	return q
}

// selectColumn sets the column to generate statistics for. Required.
// Called directly from the Client.Stats as it's a mandatory field.
func (q *statsQuery) selectColumn(column string) *statsQuery {
	q.params.Add("select", column)
	return q
}

// Filter results by only returning rows that match a search statsQuery. Multiple search parameters may be provided.
// By default this searches the entire table for matching text.
// To search particular fields only, use the statsQuery format "@fieldname statsQuery".
// To match multiple queries within a single search parameter, the | (or) operator can be used eg. "statsQuery1|statsQuery2". See the "Complex Data Search" example on the right for a demonstration.
func (q *statsQuery) Search(query string) *statsQuery {
	q.params.Add("search", query)
	return q
}

// Where filters results with a SQL-style "where" clause. Only applies to numerical and date columns – use the "search" parameter for strings. Multiple where parameters may be provided.
// <column><operator><value>
// Valid operators are >=, >, =, !=, <, and <=.
// <column> [not] in (<value>,<value>,...)
// Match rows where column matches one of the provided values.
// <column> [not] between <value> and <value>
// Match rows where column lies within range provided (inclusive).
func (q *statsQuery) Where(query string) *statsQuery {
	q.params.Add("where", query)
	return q
}

// Conjunction is only applicable when more than one "search" or "where" parameter is provided. Defaults to "and".
func (q *statsQuery) Conjunction(conjunction Conjunction) *statsQuery {
	q.params.Add("conjunction", string(conjunction))
	return q
}

func (q *statsQuery) Operation(operation Operation) *statsQuery {
	q.params.Add("operation", string(operation))
	return q
}

// Compound operation to run on a given pair of columns.
// Valid compound operations are sum and avg.
// When running a compound operation query, the "of" parameter is quired (see below).
func (q *statsQuery) By(operation Operation) *statsQuery {
	q.params.Add("by", string(operation))
	return q
}

// Numerical column to compare against when running a compound operation.
// Required when using the "by" parameter.
// Must be a numerical column.
func (q *statsQuery) Of(column string) *statsQuery {
	q.params.Add("of", column)
	return q
}

// Sort rows by a particular column in a given direction. + denotes ascending order, - denotes descending.
func (q *statsQuery) Sort(direction SortDirection) *statsQuery {
	q.params.Add("sort", string(direction))
	return q
}

// Page paginates row results and returns the nth page of results. Pages are calculated based on the current limit, which defaults to 500.
func (q *statsQuery) Page(number int) *statsQuery {
	q.params.Add("page", strconv.Itoa(number))
	return q
}

// Results or error returned by the server.
func (q *statsQuery) Results() (response *StatsResponse, err error) {
	err = doQuery(q.baseUri, q.datapath, q.params, &response)
	return
}

// DataResponse attributes
type DataResponse struct {
	DataPath string          `json:"data_path"`
	Result   json.RawMessage `json:"result"`
	Info     struct {
		RowsLimit    int `json:"rows_limit"`
		CurrentPage  int `json:"current_page"`
		TotalPages   int `json:"total_pages"`
		TotalResults int `json:"total_results"`
	} `json:"info"`
}

// Table datapaths can be queried for the data they contain.
// Data queries may be filtered, sorted and paginated using the provided URL parameters.
type dataQuery query

// Limit the number of rows returned (max. 500). Defaults to 500.
func (q *dataQuery) Limit(number int) *dataQuery {
	q.params.Add("limit", strconv.Itoa(number))
	return q
}

// Select the columns to be returned with each row. Default is to return all columns.
func (q *dataQuery) Select(columns ...string) *dataQuery {
	q.params.Add("select", strings.Join(columns, ","))
	return q
}

// Search filters the results by only returning rows that match a query.
// Multiple search parameters may be provided.
// By default this searches the entire table for matching text.
// To search particular fields only, use the dataQuery format "@fieldname dataQuery".
// To match multiple queries within a single search parameter, the | (or) operator can be used eg. "dataQuery1|dataQuery2". See the "Complex Data Search" example on the right for a demonstration.
func (q *dataQuery) Search(query string) *dataQuery {
	q.params.Add("search", query)
	return q
}

// Where filters results with a SQL-style "where" clause. Only applies to numerical and date columns – use the "search" parameter for strings. Multiple where parameters may be provided.
// <column><operator><value>
// Valid operators are >=, >, =, !=, <, and <=.
// <column> [not] in (<value>,<value>,...)
// Match rows where column matches one of the provided values.
// <column> [not] between <value> and <value>
// Match rows where column lies within range provided (inclusive).
func (q *dataQuery) Where(query string) *dataQuery {
	q.params.Add("where", query)
	return q
}

// Conjunction is only applicable when more than one "search" or "where" parameter is provided. Defaults to "and".
func (q *dataQuery) Conjunction(conjunction Conjunction) *dataQuery {
	q.params.Add("conjunction", string(conjunction))
	return q
}

// Sort rows by a particular column in a given direction.
func (q *dataQuery) Sort(column string, direction SortDirection) *dataQuery {
	q.params.Add("sort", column+string(direction))
	return q
}

// Page paginates row results and return the nth page of results.
// Pages are calculated based on the current limit, which defaults to 500.
func (q *dataQuery) Page(number int) *dataQuery {
	q.params.Add("page", strconv.Itoa(number))
	return q
}

// Results or error returned by the server.
func (q *dataQuery) Results() (response DataResponse, err error) {
	err = doQuery(q.baseUri, q.datapath, q.params, &response)
	return
}

// exportResponse attributes
type exportResponse struct {
	DataPath  string `json:"data_path"`
	ExportUrl string `json:"export_url"`
	HeadUrl   string `json:"head_url"`
}

type exportQuery query

// Select the list of columns to be returned with each row. Default is to return all columns.
func (q *exportQuery) Select(columns ...string) *exportQuery {
	q.params.Add("select", strings.Join(columns, ","))
	return q
}

// Search filters results by only returning rows that match a search query.
// Multiple search parameters may be provided.
// By default this searches the entire table for matching text.
// To search particular fields only, use the dataQuery format "@fieldname dataQuery".
// To match multiple queries within a single search parameter, the | (or) operator can be used eg. "query1|query2".
func (q *exportQuery) Search(query string) *exportQuery {
	q.params.Add("search", query)
	return q
}

// Where filters results with a SQL-style "where" clause.
// Only applies to numerical and date columns – use the "search" parameter for strings. Multiple where parameters may be provided.
// <column><operator><value>
// Valid operators are >=, >, =, !=, <, and <=.
// <column> [not] in (<value>,<value>,...)
// Match rows where column matches one of the provided values.
// <column> [not] between <value> and <value>
// Match rows where column lies within range provided (inclusive).
func (q *exportQuery) Where(query string) *exportQuery {
	q.params.Add("where", query)
	return q
}

// Conjunction is only applicable when more than one "search" or "where" parameter is provided. Defaults to "and".
func (q *exportQuery) Conjunction(conjunction Conjunction) *exportQuery {
	q.params.Add("conjunction", string(conjunction))
	return q
}

// Sort rows by a particular column in a given direction. + denotes ascending order, - denotes descending.
func (q *exportQuery) Sort(column string, direction SortDirection) *exportQuery {
	q.params.Add("sort", column+string(direction))
	return q
}

// Page paginates row results and returns the nth page of results. Pages are calculated based on the current limit, which defaults to 500.
func (q *exportQuery) Page(number int) *exportQuery {
	q.params.Add("page", strconv.Itoa(number))
	return q
}

// FileUrl returns the URL of the GZip file containing the exported data.
func (q *exportQuery) FileUrl() (url string, err error) {
	var response exportResponse
	err = doQuery(q.baseUri, q.datapath, q.params, &response)
	return response.ExportUrl, err
}

// Client of the Enigma API.
// Use NewClient to instantiate a new instance.
type Client struct {
	key  string
	Meta *metaQuery
}

func (client *Client) buildUri(ep endpoint) string {
	//<root>/<version>/<endpoint>/<api key>/<datapath>/<parameters>
	return strings.Join([]string{root, version, string(ep), client.key}, "/")
}

// Data queries the content of table datapaths.
// Data queries may be filtered, sorted and paginated using the returned request  object.
// For large tables and tables with a large number of columns, data API calls may take some time to complete.
// API users are advised to make use of the "select" and/or "limit" parameters whenever possible to improve performance.
func (client *Client) Data(datapath string) *dataQuery {
	return &dataQuery{
		datapath: datapath,
		params:   url.Values{},
		baseUri:  client.buildUri(data),
	}
}

// Stats queries table datapaths by column for statistics on the data that it contains.
// Like data queries, stats queries may be filtered, sorted and paginated using the returned request objet.
func (client *Client) Stats(datapath, column string) *statsQuery {
	q := &statsQuery{
		datapath: datapath,
		params:   url.Values{},
		baseUri:  client.buildUri(stats),
	}
	return q.selectColumn(column)
}

// Export requests exports of table datapaths as GZiped files.
func (client *Client) Export(datapath string) *exportQuery {
	return &exportQuery{
		datapath: datapath,
		params:   url.Values{},
		baseUri:  client.buildUri(export),
	}
}

// NewClient instantiates a new Client instance with a given API key.
func NewClient(key string) (instance *Client) {
	instance = &Client{
		key: key,
	}

	instance.Meta = &metaQuery{
		baseUri: instance.buildUri(meta),
	}

	return instance
}
