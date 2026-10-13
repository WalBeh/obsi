package cratedb

// RepositoryKey is a setting a repository type accepts, as CrateDB's
// TypeSettings define them (6.4: RepositoriesModule, the s3, azure, gcs
// and url plugins).
type RepositoryKey struct {
	Name     string
	Default  string // as the docs give it; "" when there is none
	Kind     string // bool, int, size, time, protocol or text: what obsi checks before running
	Required bool   // can't be reset
	Secret   bool   // masked in sys.repositories as [xxxxx]
	// Place is part of where the snapshots live: changed, the repository
	// lists what is at the new place.
	Place bool
}

// MaskedValue is how sys.repositories shows a secret setting.
const MaskedValue = "[xxxxx]"

var genericRepositoryKeys = []RepositoryKey{
	{Name: "max_snapshot_bytes_per_sec", Default: "40mb", Kind: "size"},
	{Name: "max_restore_bytes_per_sec", Default: "40mb", Kind: "size"},
}

var repositoryKeys = map[string][]RepositoryKey{
	"fs": {
		{Name: "location", Kind: "text", Required: true, Place: true},
		{Name: "compress", Default: "true", Kind: "bool"},
		{Name: "chunk_size", Kind: "size"},
		{Name: "readonly", Default: "false", Kind: "bool"},
	},
	"url": {
		{Name: "url", Kind: "text", Required: true, Place: true},
	},
	"s3": {
		{Name: "bucket", Kind: "text", Place: true},
		{Name: "base_path", Kind: "text", Place: true},
		{Name: "endpoint", Kind: "text", Place: true},
		{Name: "region", Kind: "text"},
		{Name: "protocol", Default: "https", Kind: "protocol"},
		{Name: "access_key", Kind: "text", Secret: true},
		{Name: "secret_key", Kind: "text", Secret: true},
		{Name: "use_path_style_access", Kind: "bool"},
		{Name: "compress", Default: "true", Kind: "bool"},
		{Name: "chunk_size", Kind: "size"},
		{Name: "buffer_size", Default: "5mb", Kind: "size"},
		{Name: "server_side_encryption", Default: "false", Kind: "bool"},
		{Name: "storage_class", Default: "standard", Kind: "text"},
		{Name: "max_retries", Default: "3", Kind: "int"},
		{Name: "use_throttle_retries", Default: "true", Kind: "bool"},
		{Name: "readonly", Default: "false", Kind: "bool"},
	},
	"azure": {
		{Name: "account", Kind: "text", Required: true, Secret: true, Place: true},
		{Name: "container", Default: "crate-snapshots", Kind: "text", Place: true},
		{Name: "base_path", Kind: "text", Place: true},
		{Name: "endpoint", Kind: "text", Place: true},
		{Name: "key", Kind: "text", Secret: true},
		{Name: "sas_token", Kind: "text", Secret: true},
		{Name: "compress", Default: "true", Kind: "bool"},
		{Name: "chunk_size", Default: "256mb", Kind: "size"},
		{Name: "max_retries", Default: "3", Kind: "int"},
		{Name: "readonly", Default: "false", Kind: "bool"},
	},
	"gcs": {
		{Name: "bucket", Kind: "text", Required: true, Place: true},
		{Name: "base_path", Kind: "text", Place: true},
		{Name: "endpoint", Kind: "text", Place: true},
		{Name: "project_id", Kind: "text", Required: true},
		{Name: "client_email", Kind: "text", Required: true, Secret: true},
		{Name: "client_id", Kind: "text", Required: true, Secret: true},
		{Name: "private_key_id", Kind: "text", Required: true, Secret: true},
		{Name: "private_key", Kind: "text", Required: true, Secret: true},
		{Name: "compress", Default: "true", Kind: "bool"},
		{Name: "chunk_size", Kind: "size"},
		{Name: "connect_timeout", Default: "0", Kind: "time"},
		{Name: "read_timeout", Default: "0", Kind: "time"},
	},
}

// RepositoryKeys lists the settings a repository type accepts, the type's
// own first, then the throttles every type has. Nil for a type obsi
// doesn't know.
func RepositoryKeys(typ string) []RepositoryKey {
	own, ok := repositoryKeys[typ]
	if !ok {
		return nil
	}
	return append(append([]RepositoryKey(nil), own...), genericRepositoryKeys...)
}

// CanToggleReadOnly is false for url, which is always read-only, and gcs,
// which doesn't take the key.
func CanToggleReadOnly(typ string) bool {
	for _, k := range repositoryKeys[typ] {
		if k.Name == "readonly" {
			return true
		}
	}
	return false
}
