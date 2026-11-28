package dbmcp

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	RowLimit     = 50
	ByteLimit    = 32 * 1024
	queryTimeout = 15 * time.Second
)

var (
	identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9]{0,62}$`)
	targetName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
)

type refusal string

func (r refusal) Error() string { return string(r) }

type Target struct {
	Name           string
	Engine         string
	Environment    string
	Host           string
	Port           int
	Database       string
	Schema         string
	Username       string
	PasswordFile   string `toml:"password_file"`
	TLS            string
	CAFile         string `toml:"ca_file"`
	ReplicaSet     string `toml:"replica_set"`
	ReadPreference string `toml:"read_preference"`
	AuthDatabase   string `toml:"auth_database"`
	Objects        map[string][]string
	passwordValue  string
}

type Catalog struct {
	Targets []*Target
	byName  map[string]*Target
}

func LoadCatalog(path string) (*Catalog, error) {
	var config struct{ Targets []*Target }
	meta, err := toml.DecodeFile(path, &config)
	if err != nil {
		return nil, fmt.Errorf("read catalog: %w", err)
	}
	if !meta.IsDefined("targets") || len(meta.Undecoded()) != 0 {
		return nil, errors.New("catalog needs a targets list and recognized keys")
	}
	directory, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	return newCatalog(config.Targets, directory)
}

// NewMySQLReader uses an executor-owned credential in memory. The configured
// target always verifies the server certificate and its hostname.
func NewMySQLReader(name, host string, port int, caFile, password string) (*Catalog, error) {
	if caFile == "" || password == "" {
		return nil, errors.New("the pilot needs a private CA and reader credential")
	}
	return newCatalog([]*Target{{Name: name, Engine: "mysql", Environment: "test", Host: host,
		Port: port, Database: "meter_demo", Username: "meter_reader", TLS: "verify", CAFile: caFile,
		passwordValue: password, Objects: map[string][]string{"readings": {"sequence_no", "station", "state", "watts"}}}}, "")
}

func newCatalog(targets []*Target, directory string) (*Catalog, error) {
	catalog := &Catalog{Targets: targets, byName: make(map[string]*Target)}
	for _, t := range targets {
		if t == nil || !targetName.MatchString(t.Name) || catalog.byName[t.Name] != nil {
			return nil, errors.New("each target needs a unique lowercase name")
		}
		if err := t.configure(directory); err != nil {
			return nil, fmt.Errorf("target %s: %w", t.Name, err)
		}
		catalog.byName[t.Name] = t
	}
	return catalog, nil
}

func resolve(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}

func (t *Target) configure(directory string) error {
	if !slices.Contains([]string{"local", "dev", "test", "qa", "staging"}, t.Environment) ||
		!slices.Contains([]string{"postgres", "mysql", "mongodb"}, t.Engine) {
		return errors.New("unknown engine or non-production environment")
	}
	if t.Host == "" || t.Port < 1 || t.Port > 65535 || t.Username == "" || (t.PasswordFile == "" && t.passwordValue == "") {
		return errors.New("supply host, port, username and password_file")
	}
	if t.Schema == "" {
		t.Schema = t.Database
		if t.Engine == "postgres" {
			t.Schema = "public"
		}
	}
	if !identifier.MatchString(t.Database) || !identifier.MatchString(t.Schema) ||
		(t.Engine == "mysql" && t.Schema != t.Database) {
		return errors.New("invalid database or schema")
	}
	if len(t.Objects) == 0 {
		return errors.New("empty object catalog")
	}
	for object, fields := range t.Objects {
		if !identifier.MatchString(object) || len(fields) == 0 {
			return errors.New("invalid catalog object")
		}
		seen := make(map[string]bool)
		for _, f := range fields {
			key := f
			if t.Engine == "mysql" {
				key = strings.ToLower(f)
			}
			if !identifier.MatchString(f) || seen[key] {
				return errors.New("invalid or repeated catalog field")
			}
			seen[key] = true
		}
	}
	if t.TLS == "" {
		t.TLS = "verify"
	}
	localLoopback := t.TLS == "local" && t.Environment == "local" && (t.Host == "127.0.0.1" || t.Host == "::1")
	if t.TLS != "verify" && !localLoopback {
		return errors.New("unencrypted connections require a local target with a loopback IP")
	}
	t.PasswordFile = resolve(directory, t.PasswordFile)
	t.CAFile = resolve(directory, t.CAFile)
	if t.AuthDatabase == "" {
		t.AuthDatabase = t.Database
	}
	if t.ReadPreference == "" {
		t.ReadPreference = "primary"
		if t.ReplicaSet != "" {
			t.ReadPreference = "secondary"
		}
	}
	if !slices.Contains([]string{"primary", "secondary", "secondaryPreferred"}, t.ReadPreference) {
		return errors.New("invalid read preference")
	}
	return nil
}

func (t *Target) password() (string, error) {
	if t.passwordValue != "" {
		return t.passwordValue, nil
	}
	data, err := os.ReadFile(t.PasswordFile)
	return strings.TrimRight(string(data), "\r\n"), err
}

func (t *Target) fields(object string) ([]string, error) {
	fields, ok := t.Objects[object]
	if !ok {
		return nil, refusal("Object is outside the catalog.")
	}
	return fields, nil
}

// Each driver verifies the hostname it actually connects to, including replica set members.
func (t *Target) tlsConfig() (*tls.Config, error) {
	if t.TLS == "local" {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CAFile != "" {
		data, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, err
		}
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM(data) {
			return nil, errors.New("invalid CA certificate")
		}
	}
	return config, nil
}
