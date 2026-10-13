package cratedb

import "testing"

func TestRepositoryKeys(t *testing.T) {
	keys := map[string]RepositoryKey{}
	for _, k := range RepositoryKeys("azure") {
		keys[k.Name] = k
	}
	if a := keys["account"]; !a.Required || !a.Secret || !a.Place {
		t.Errorf("azure account = %+v", a)
	}
	if !keys["key"].Secret || !keys["sas_token"].Secret || keys["max_snapshot_bytes_per_sec"].Default != "40mb" {
		t.Errorf("azure keys = %+v", keys)
	}
	if RepositoryKeys("hdfs") != nil {
		t.Error("unknown type should have no keys")
	}
	for typ, want := range map[string]bool{"fs": true, "s3": true, "azure": true, "url": false, "gcs": false} {
		if CanToggleReadOnly(typ) != want {
			t.Errorf("CanToggleReadOnly(%s) = %v", typ, !want)
		}
	}
}
