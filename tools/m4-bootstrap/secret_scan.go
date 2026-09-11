package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf16"
)

type scanEntry struct {
	Name           string `json:"name"`
	Representation string `json:"representation"`
	Length         int    `json:"byte_length"`
	Digest         string `json:"sha256"`
}
type secretScan struct {
	Version     int         `json:"schema_version"`
	RunID       string      `json:"run_id"`
	Commit      string      `json:"commit"`
	SessionHash string      `json:"session_sha256"`
	Profile     string      `json:"profile"`
	State       string      `json:"state"`
	Entries     []scanEntry `json:"entries"`
	path        string
}

func secretRepresentations(value string) map[string]string {
	encoded, _ := json.Marshal(value)
	goJSON := string(encoded[1 : len(encoded)-1])
	var ascii strings.Builder
	for _, r := range value {
		if r >= 127 {
			for _, u := range utf16.Encode([]rune{r}) {
				_, _ = fmt.Fprintf(&ascii, `\u%04x`, u)
			}
		} else {
			part, _ := json.Marshal(string(r))
			// Python's default encoder does not HTML-escape these characters.
			if r == '<' || r == '>' || r == '&' {
				ascii.WriteRune(r)
			} else {
				ascii.Write(part[1 : len(part)-1])
			}
		}
	}
	return map[string]string{
		"raw": value, "json-go": goJSON, "json-ascii": ascii.String(),
		"userinfo": encodedPassword(value), "url-query": url.QueryEscape(value),
		"base64":    base64.StdEncoding.EncodeToString([]byte(value)),
		"base64url": base64.RawURLEncoding.EncodeToString([]byte(value)),
	}
}

func openSecretScan(root, runID, commit string) (*secretScan, error) {
	raw := filepath.Join(root, ".evidence", "m4", runID)
	// A permanent claim makes bootstrap a single attempt, including partial failures.
	claim, err := os.OpenFile(filepath.Join(raw, "bootstrap.claim"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, errors.New("bootstrap already claimed or claim cannot be persisted")
	}
	if err := claim.Close(); err != nil {
		return nil, err
	}
	path := filepath.Join(raw, "secret-scan.json")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > 32768 {
		return nil, errors.New("private secret scan receipt is missing or unsafe")
	}
	var receipt secretScan
	data, err := os.ReadFile(path)
	if err != nil || !validInitialScanJSON(data) {
		return nil, errors.New("invalid secret scan receipt JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return nil, errors.New("invalid secret scan receipt fields")
	}
	session, err := os.ReadFile(filepath.Join(raw, "00-session.json"))
	if err != nil {
		return nil, err
	}
	if receipt.Version != 1 || receipt.RunID != runID || receipt.Commit != commit || receipt.Profile != "m4-secret-representations-v1" || receipt.SessionHash != fmt.Sprintf("%x", sha256.Sum256(session)) {
		return nil, errors.New("secret scan receipt binding mismatch")
	}
	if receipt.State != "not_started" || len(receipt.Entries) != 0 {
		return nil, errors.New("bootstrap has already begun for this run; stop the attempt")
	}
	receipt.path = path
	return &receipt, nil
}

// Initial receipts have an empty entries array, so rejecting duplicate outer
// keys and enforcing typed fields also covers every nested receipt value.
func validInitialScanJSON(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return false
		}
		if key == "entries" && string(bytes.TrimSpace(value)) != "[]" {
			return false
		}
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	_, err := decoder.Token()
	return len(seen) == 7 && err == io.EOF
}

func (s *secretScan) save() error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), ".secret-scan-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err = f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), s.path)
}
func (s *secretScan) begin() error { s.State = "collecting"; return s.save() }
func (s *secretScan) record(name, value string) error {
	if s.State != "collecting" || len(value) < 16 || len(value) > 4096 {
		return errors.New("invalid sensitive value or scan state")
	}
	for representation, text := range secretRepresentations(value) {
		// SHA-256 is an equality fingerprint for leak detection over AWS-generated
		// high-entropy credentials, not a password verifier or credential store.
		// codeql[go/weak-sensitive-data-hashing]
		s.Entries = append(s.Entries, scanEntry{name, representation, len([]byte(text)), fmt.Sprintf("%x", sha256.Sum256([]byte(text)))})
	}
	return s.save()
}
func (s *secretScan) complete() error {
	if len(s.Entries) != 21 {
		return errors.New("secret scan coverage incomplete")
	}
	s.State = "complete"
	return s.save()
}
