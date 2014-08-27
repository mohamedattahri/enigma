package enigma

import (
	"fmt"
	"testing"
)

const (
	key      string = "VNVEhSReVMUGUw9SEdd6ZNJmRtptRl4uPOfTHyINKMgNPaisVmhFH"
	datapath string = "us.gov.whitehouse.visitor-list"
)

var (
	client = NewClient(key)
)

func Example_meta() {
	client := enigma.NewClient("some_api_key")
	response, err := client.Meta.Table("us.gov.whitehouse.visitor-list")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(response.Result.DbBoundaryLabel)
}

func Example_data() {
	client := enigma.NewClient("some_api_key")
	response, err := client.Data("us.gov.whitehouse.visitor-list").Select("namefull", "appt_made_date").Sort("namefirst", enigma.Desc).Results()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(response.Result))
}

func Example_stats() {
	client := enigma.NewClient("some_api_key")
	response, err := client.Stats("us.gov.whitehouse.visitor-list", "total_people").Operation(enigma.Sum).Results()
	if err != nil {
		fmt.Println(err)
		return
	}

	var obj map[string]string
	json.Unmarshal(response.Result, &obj)
	fmt.Println(obj["sum"])
}

func Example_export() {
	client := enigma.NewClient("some_api_key")
	url, err := client.Export("us.gov.whitehouse.visitor-list").FileUrl()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(url)
}

func TestUrlBuilding(t *testing.T) {
	query1 := client.Data("us.gov.whitehouse.visitor-list").Select("namefull", "appt_made_date").Sort("namefirst", Desc)
	uri1 := buildUrl(query1.baseUri, query1.datapath, query1.params)
	if uri1 != "https://api.enigma.io/v2/data/VNVEhSReVMUGUw9SEdd6ZNJmRtptRl4uPOfTHyINKMgNPaisVmhFH/us.gov.whitehouse.visitor-list?select=namefull%2Cappt_made_date&sort=namefirst-" {
		t.Fatal(uri1)
	}

	query2 := client.Stats("us.gov.whitehouse.visitor-list", "total_people").Operation(Sum)
	uri2 := buildUrl(query2.baseUri, query2.datapath, query2.params)
	if uri2 != "https://api.enigma.io/v2/stats/VNVEhSReVMUGUw9SEdd6ZNJmRtptRl4uPOfTHyINKMgNPaisVmhFH/us.gov.whitehouse.visitor-list?operation=sum&select=total_people" {
		t.Fatal(uri2)
	}

	query3 := client.Export("us.gov.whitehouse.visitor-list").Select("namefull")
	uri3 := buildUrl(query3.baseUri, query3.datapath, query3.params)
	if uri3 != "https://api.enigma.io/v2/export/VNVEhSReVMUGUw9SEdd6ZNJmRtptRl4uPOfTHyINKMgNPaisVmhFH/us.gov.whitehouse.visitor-list?select=namefull" {
		t.Fatal(uri3)
	}
}

func TestApiError(t *testing.T) {
	_, err := client.Data("us.gov.whitehouse.visitor-list").Select("blablabla").Results()
	if err == nil {
		t.Fatal("Expected error was not returned")
	}
}

func TestMetaQuery(t *testing.T) {
	if client.Meta == nil {
		t.Fatal("Meta query object is not accessible")
	}
}

func TestDataQuery(t *testing.T) {
	if client.Data(datapath) == nil {
		t.Fatal("Data query object is not accessible")
	}
}

func TestDataQueryConjunction(t *testing.T) {
	query := client.Data(datapath).Conjunction(Or)
	if query.params.Get("conjunction") != string(Or) {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestDataQueryLimit(t *testing.T) {
	query := client.Data(datapath).Limit(100)
	if query.params.Get("limit") != "100" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestDataQueryPage(t *testing.T) {
	query := client.Data(datapath).Page(2)
	if query.params.Get("page") != "2" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestDataQuerySearch(t *testing.T) {
	query := client.Data(datapath).Search("hello")
	if query.params.Get("search") != "hello" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestDataQuerySelect(t *testing.T) {
	query := client.Data(datapath).Select("column")
	if query.params.Get("select") != "column" {
		t.Fatal("Parameter was not properly added to the query")
	}

	m_query := client.Data(datapath).Select("column1", "column2", "column3")
	if m_query.params.Get("select") != "column1,column2,column3" {
		t.Fatal("Parameter with multiple values was not added properly to the query")
	}
}

func TestDataQuerySort(t *testing.T) {
	query := client.Data(datapath).Sort("column", Asc)
	if query.params.Get("sort") != "column+" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestDataQueryWhere(t *testing.T) {
	query := client.Data(datapath).Where("is charlie?")
	if query.params.Get("where") != "is charlie?" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQuery(t *testing.T) {
	query := client.Stats(datapath, "column")
	if query.params.Get("select") != "column" {
		t.Fatal("StatsQuery was not returned a select parameter set to the given column")
	}
}

func TestStatsQueryConjunction(t *testing.T) {
	query := client.Stats(datapath, "column").Conjunction(And)
	if query.params.Get("conjunction") != string(And) {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQueryLimit(t *testing.T) {
	query := client.Stats(datapath, "column").Limit(100)
	if query.params.Get("limit") != "100" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQueryPage(t *testing.T) {
	query := client.Stats(datapath, "column").Page(2)
	if query.params.Get("page") != "2" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQuerySearch(t *testing.T) {
	query := client.Stats(datapath, "column").Search("hello")
	if query.params.Get("search") != "hello" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQuerySort(t *testing.T) {
	query := client.Stats(datapath, "column").Sort(Desc)
	if query.params.Get("sort") != "-" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQueryWhere(t *testing.T) {
	query := client.Stats(datapath, "column").Where("is charlie?")
	if query.params.Get("where") != "is charlie?" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQueryBy(t *testing.T) {
	query := client.Stats(datapath, "column").By(Sum)
	if query.params.Get("by") != "sum" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQueryOf(t *testing.T) {
	query := client.Stats(datapath, "column").Of("column")
	if query.params.Get("of") != "column" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestStatsQueryOperation(t *testing.T) {
	query := client.Stats(datapath, "column").Operation(Avg)
	if query.params.Get("operation") != "avg" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestExportQuery(t *testing.T) {
	if client.Export(datapath) == nil {
		t.Fatal("Export query object is not accessible")
	}
}

func TestExportQueryConjunction(t *testing.T) {
	query := client.Export(datapath).Conjunction(Or)
	if query.params.Get("conjunction") != string(Or) {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestExportQuerySearch(t *testing.T) {
	query := client.Export(datapath).Search("hello")
	if query.params.Get("search") != "hello" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestExportQuerySelect(t *testing.T) {
	query := client.Export(datapath).Select("column")
	if query.params.Get("select") != "column" {
		t.Fatal("Parameter was not properly added to the query")
	}

	m_query := client.Export(datapath).Select("column1", "column2", "column3")
	if m_query.params.Get("select") != "column1,column2,column3" {
		t.Fatal("Parameter with multiple values was not added properly to the query")
	}
}

func TestExportQuerySort(t *testing.T) {
	query := client.Export(datapath).Sort("column", Asc)
	if query.params.Get("sort") != "column+" {
		t.Fatal("Parameter was not properly added to the query")
	}
}

func TestExportQueryWhere(t *testing.T) {
	query := client.Export(datapath).Where("is charlie?")
	if query.params.Get("where") != "is charlie?" {
		t.Fatal("Parameter was not properly added to the query")
	}
}
