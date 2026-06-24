package state

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Accounts captures the external identities the running orchestrator is
// configured with. Written once at startup to <state_dir>/accounts.yaml
// so introspection tools (chitra) can surface them without
// re-querying GitHub. Missing fields render as empty strings — the
// startup wiring is best-effort and a partial accounts file is still
// useful.
type Accounts struct {
	GitHubLogin  string `yaml:"github_login"`
	AnthropicWho string `yaml:"anthropic_account"`
	LinearEmail  string `yaml:"linear_email"`
	WrittenAt    string `yaml:"written_at"`
}

// AccountsPath returns the canonical path for accounts.yaml.
func AccountsPath(root string) string {
	return filepath.Join(root, "accounts.yaml")
}

// WriteAccounts serialises a in YAML to <root>/accounts.yaml. Overwrites
// on every call (the orchestrator writes it once per startup).
func WriteAccounts(root string, a Accounts) error {
	raw, err := yaml.Marshal(a)
	if err != nil {
		return fmt.Errorf("marshal accounts: %w", err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("ensure state root: %w", err)
	}
	return os.WriteFile(AccountsPath(root), raw, 0o644)
}

// ReadAccounts loads <root>/accounts.yaml. Returns an empty Accounts
// (no error) when the file is absent — callers should treat "no file"
// and "all fields empty" the same way ("not available").
func ReadAccounts(root string) (Accounts, error) {
	raw, err := os.ReadFile(AccountsPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return Accounts{}, nil
		}
		return Accounts{}, fmt.Errorf("read accounts: %w", err)
	}
	var a Accounts
	if err := yaml.Unmarshal(raw, &a); err != nil {
		return Accounts{}, fmt.Errorf("parse accounts: %w", err)
	}
	return a, nil
}
