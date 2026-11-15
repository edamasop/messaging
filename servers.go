package messaging

import "strings"

// splitServers turns the comma-separated BootstrapServers value into the list
// of individual broker addresses that kafka-go expects.
//
// Without this, a config like "broker-a:9092,broker-b:9092" would be passed to
// kafka.TCP as a single hostname, so the client would try to resolve an address
// that does not exist and every produce/fetch would fail with a DNS error.
func splitServers(raw string) []string {
	parts := strings.Split(raw, ",")

	servers := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		server := strings.TrimSpace(part)
		if server == "" {
			continue
		}
		if _, duplicate := seen[server]; duplicate {
			continue
		}

		seen[server] = struct{}{}
		servers = append(servers, server)
	}

	// Never hand an empty slice to kafka-go: it would fall back to its own
	// default (localhost:9092) and silently ignore the misconfiguration.
	if len(servers) == 0 {
		return []string{raw}
	}

	return servers
}
