// Package enigma provides developers with a Go client for the Enigma.io API.
//
// The Enigma API allows users to download datasets, query metadata, or perform server side operations on tables in Enigma.
// All calls to the API are made through a RESTful protocol and require an API key.
// The Enigma API is served over HTTPS.
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

// Although used in a single location, this function has been isolated to make the code
// easier to test.
func buildUrl(baseUri, datapath string, params url.Values) string {
	uri := baseUri + "/" + datapath
	if len(params) > 0 {
		uri += "?" + params.Encode()
	}
	return uri
}

// doQuery performs the actual HTTP request and parses the returned JSON into a typed response structure.
func doQuery(baseUri, datapath string, params url.Values, response interface{}) (err error) {
	uri := buildUrl(baseUri, datapath, params)

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

// MetaParentNodeResponse represents the structure of a metadata response describing a parent node.
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

// MetaTableNodeResponse represents the structure of a metadata response describing a table.
type MetaTableNodeResponse struct {
	DataPath string `json:"datapath"`
	Result   struct {
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

// MetaQuery can be used on all datapaths to query their metadata.
type MetaQuery query

// Parent metadata request for the given datapath.
func (q *MetaQuery) Parent(datapath string) (response *MetaParentNodeResponse, err error) {
	err = doQuery(q.baseUri, datapath, q.params, &response)
	return
}

// Table metadata request for the given metadata.
func (q *MetaQuery) Table(datapath string) (response *MetaTableNodeResponse, err error) {
	err = doQuery(q.baseUri, datapath, q.params, &response)
	return
}

// StatsResponse represents the response returned from a Stats endpoint.
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

// StatsQuery can be used to query columns of tables for statistics on the data they contain.
// Like data queries, stats queries may be filtered, sorted and paginated using the provided URL parameters.
type StatsQuery query

// selectColumn sets the column to generate statistics for. Required.
// Called directly from the Client.Stats as it's a mandatory field.
func (q *StatsQuery) selectColumn(column string) *StatsQuery {
	q.params.Add("select", column)
	return q
}

// Limit the number of frequency, compound sum, or compound average results returned (max. 500).
// Defaults to 500.
func (q *StatsQuery) Limit(limit int) *StatsQuery {
	q.params.Add("limit", strconv.Itoa(limit))
	return q
}

// Filter results by only returning rows that match a search StatsQuery. Multiple search parameters may be provided.
// By default this searches the entire table for matching text.
// To search particular fields only, use the StatsQuery format "@fieldname StatsQuery".
// To match multiple queries within a single search parameter, the | (or) operator can be used eg. "StatsQuery1|StatsQuery2". See the "Complex Data Search" example on the right for a demonstration.
func (q *StatsQuery) Search(query string) *StatsQuery {
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
func (q *StatsQuery) Where(query string) *StatsQuery {
	q.params.Add("where", query)
	return q
}

// Conjunction is only applicable when more than one "search" or "where" parameter is provided. Defaults to "and".
func (q *StatsQuery) Conjunction(conjunction Conjunction) *StatsQuery {
	q.params.Add("conjunction", string(conjunction))
	return q
}

// Operation to run the given column.
// For a numerical column, valid operations are sum, avg, stddev, variance, max, min and frequency.
// For a date column, valid operations are max,min and frequency.
// For all other columns, the only valid operation is frequency.
// Defaults to all available operations based on the column's type.
func (q *StatsQuery) Operation(operation Operation) *StatsQuery {
	q.params.Add("operation", string(operation))
	return q
}

// By indicates the compound operation to run on a given pair of columns.
// Valid compound operations are sum and avg.
// When running a compound operation query, the "of" parameter is required (see below).
func (q *StatsQuery) By(operation Operation) *StatsQuery {
	q.params.Add("by", string(operation))
	return q
}

// Of indicates the numerical column to compare against when running a compound operation.
// Required when using the "by" parameter. Must be a numerical column.
func (q *StatsQuery) Of(column string) *StatsQuery {
	q.params.Add("of", column)
	return q
}

// Sort rows by a particular column in a given direction. + denotes ascending order, - denotes descending.
func (q *StatsQuery) Sort(direction SortDirection) *StatsQuery {
	q.params.Add("sort", string(direction))
	return q
}

// Page paginates row results and returns the nth page of results. Pages are calculated based on the current limit, which defaults to 500.
func (q *StatsQuery) Page(number int) *StatsQuery {
	q.params.Add("page", strconv.Itoa(number))
	return q
}

// Results or error returned by the server.
func (q *StatsQuery) Results() (response *StatsResponse, err error) {
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

// DataQuery queries table datapaths for the data they contain.
// Data queries may be filtered, sorted and paginated using the provided URL parameters.
type DataQuery query

// Limit the number of rows returned (max. 500). Defaults to 500.
func (q *DataQuery) Limit(number int) *DataQuery {
	q.params.Add("limit", strconv.Itoa(number))
	return q
}

// Select the columns to be returned with each row. Default is to return all columns.
func (q *DataQuery) Select(columns ...string) *DataQuery {
	q.params.Add("select", strings.Join(columns, ","))
	return q
}

// Search filters the results by only returning rows that match a query.
// Multiple search parameters may be provided.
// By default this searches the entire table for matching text.
// To search particular fields only, use the query format "@fieldname query".
// To match multiple queries within a single search parameter, the | (or) operator can be used eg. "DataQuery1|DataQuery2". See the "Complex Data Search" example on the right for a demonstration.
func (q *DataQuery) Search(query string) *DataQuery {
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
func (q *DataQuery) Where(query string) *DataQuery {
	q.params.Add("where", query)
	return q
}

// Conjunction is only applicable when more than one "search" or "where" parameter is provided. Defaults to "and".
func (q *DataQuery) Conjunction(conjunction Conjunction) *DataQuery {
	q.params.Add("conjunction", string(conjunction))
	return q
}

// Sort rows by a particular column in a given direction.
func (q *DataQuery) Sort(column string, direction SortDirection) *DataQuery {
	q.params.Add("sort", column+string(direction))
	return q
}

// Page paginates row results and return the nth page of results.
// Pages are calculated based on the current limit, which defaults to 500.
func (q *DataQuery) Page(number int) *DataQuery {
	q.params.Add("page", strconv.Itoa(number))
	return q
}

// Results or error returned by the server.
func (q *DataQuery) Results() (response DataResponse, err error) {
	err = doQuery(q.baseUri, q.datapath, q.params, &response)
	return
}

// exportResponse attributes
type exportResponse struct {
	DataPath  string `json:"data_path"`
	ExportUrl string `json:"export_url"`
	HeadUrl   string `json:"head_url"`
}

type ExportQuery query

// Select the list of columns to be returned with each row. Default is to return all columns.
func (q *ExportQuery) Select(columns ...string) *ExportQuery {
	q.params.Add("select", strings.Join(columns, ","))
	return q
}

// Search filters results by only returning rows that match a search query.
// Multiple search parameters may be provided.
// By default this searches the entire table for matching text.
// To search particular fields only, use the DataQuery format "@fieldname DataQuery".
// To match multiple queries within a single search parameter, the | (or) operator can be used eg. "query1|query2".
func (q *ExportQuery) Search(query string) *ExportQuery {
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
func (q *ExportQuery) Where(query string) *ExportQuery {
	q.params.Add("where", query)
	return q
}

// Conjunction is only applicable when more than one "search" or "where" parameter is provided. Defaults to "and".
func (q *ExportQuery) Conjunction(conjunction Conjunction) *ExportQuery {
	q.params.Add("conjunction", string(conjunction))
	return q
}

// Sort rows by a particular column in a given direction. + denotes ascending order, - denotes descending.
func (q *ExportQuery) Sort(column string, direction SortDirection) *ExportQuery {
	q.params.Add("sort", column+string(direction))
	return q
}

// Page paginates row results and returns the nth page of results. Pages are calculated based on the current limit, which defaults to 500.
func (q *ExportQuery) Page(number int) *ExportQuery {
	q.params.Add("page", strconv.Itoa(number))
	return q
}

// FileUrl returns the URL of the GZip file containing the exported data.
func (q *ExportQuery) FileUrl() (url string, err error) {
	var response exportResponse
	err = doQuery(q.baseUri, q.datapath, q.params, &response)
	return response.ExportUrl, err
}

// Client of the Enigma API.
// Use NewClient to instantiate a new instance.
type Client struct {
	key string
	// Meta can be used to query all datapaths for their metadata.
	Meta *MetaQuery
}

func (client *Client) buildUri(ep endpoint) string {
	//<root>/<version>/<endpoint>/<api key>/<datapath>/<parameters>
	return strings.Join([]string{root, version, string(ep), client.key}, "/")
}

// Data queries the content of table datapaths.
// Data queries may be filtered, sorted and paginated using the returned request  object.
// For large tables and tables with a large number of columns, data API calls may take some time to complete.
// API users are advised to make use of the "select" and/or "limit" parameters whenever possible to improve performance.
func (client *Client) Data(datapath string) *DataQuery {
	return &DataQuery{
		datapath: datapath,
		params:   url.Values{},
		baseUri:  client.buildUri(data),
	}
}

// Stats queries table datapaths by column for statistics on the data that it contains.
// Like data queries, stats queries may be filtered, sorted and paginated using the returned request objet.
func (client *Client) Stats(datapath, column string) *StatsQuery {
	q := &StatsQuery{
		datapath: datapath,
		params:   url.Values{},
		baseUri:  client.buildUri(stats),
	}
	return q.selectColumn(column)
}

// Export requests exports of table datapaths as GZiped files.
func (client *Client) Export(datapath string) *ExportQuery {
	return &ExportQuery{
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

	instance.Meta = &MetaQuery{
		baseUri: instance.buildUri(meta),
	}

	return instance
}
