package cratedb

import (
	"strings"
	"testing"
)

// A suspended CrateDB Cloud cluster: Traefik answers for the removed route.
func TestCrateDBErrorNotCrateDB(t *testing.T) {
	cloud := &CrateDBError{StatusCode: 404, Endpoint: "https://c1.aks1.westeurope.azure.cratedb.net:4200", Body: "404 page not found\n"}
	if got := cloud.Error(); got != `https://c1.aks1.westeurope.azure.cratedb.net:4200 answered 404 "404 page not found", which isn't CrateDB: something in front of it has no route to the cluster. On CrateDB Cloud a suspended cluster looks like this (suspending removes its route): resume it in the console` {
		t.Errorf("cloud 404: %s", got)
	}
	other := &CrateDBError{StatusCode: 404, Endpoint: "https://lb.example.com.:4200", Body: "Not Found"}
	if got := other.Error(); !strings.Contains(got, "no route to the cluster") || strings.Contains(got, "Cloud") {
		t.Errorf("proxy 404: %s", got)
	}
	// CrateDB's own errors keep their text: JSON 404s, plain-text 401s.
	for _, e := range []*CrateDBError{
		{StatusCode: 404, Endpoint: "http://h:4200", Body: `{"error":{"message":"RelationUnknown[x]","code":4041}}`},
		{StatusCode: 401, Endpoint: "http://h:4200", Body: `password authentication failed for user "nobody"`},
	} {
		if e.NotCrateDB() || !strings.HasPrefix(e.Error(), "cratedb error") {
			t.Errorf("CrateDB's own error rewritten: %s", e.Error())
		}
	}
}
