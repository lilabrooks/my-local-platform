//go:build unix

// Package fixture manages only the disposable databases owned by one checkout.
package fixture

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

var users = []string{"pg-owner", "pg-reader", "my-owner", "my-reader", "mg-owner", "mg-reader"}

func Project(root string) (string, error) {
	path, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(path))
	return "mlp-db-mcp-" + hex.EncodeToString(sum[:5]), nil
}

// The temporary file and rename prevent an interrupted startup leaving a partial password.
func createPassword(path string) error {
	var buf [24]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".password-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err = f.WriteString(hex.EncodeToString(buf[:])); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func Run(root, action string, stdout, stderr io.Writer) error {
	if action != "up" && action != "down" {
		return errors.New("choose up or down")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	project, err := Project(root)
	if err != nil {
		return err
	}
	state := filepath.Join(root, "local", "tmp", "db-mcp")
	if err = os.MkdirAll(state, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(state, ".lifecycle"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	env := os.Environ()
	for _, user := range users {
		path := filepath.Join(state, user)
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			if action == "up" {
				if err = createPassword(path); err != nil {
					return err
				}
				data, err = os.ReadFile(path)
			} else {
				data, err = []byte("unused"), nil
			}
		}
		if err != nil {
			return err
		}
		key := "METER_" + strings.ToUpper(strings.ReplaceAll(user, "-", "_"))
		env = append(env, key+"="+string(data))
	}
	args := []string{"compose", "-p", project, "-f", filepath.Join(root, "local", "docker-compose.db-mcp.yml")}
	if action == "up" {
		args = append(args, "up", "-d", "--wait", "--wait-timeout", "240")
	} else {
		args = append(args, "down", "--volumes")
	}
	cmd := exec.Command("docker", args...)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, stdout, stderr
	if err = cmd.Run(); err != nil {
		return fmt.Errorf("compose %s: %w", action, err)
	}
	if action == "down" {
		for _, user := range users {
			if err = os.Remove(filepath.Join(state, user)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}
