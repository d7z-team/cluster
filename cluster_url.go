package cluster

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// NewClusterFromURL creates a Cluster from a backend URL.
//
// Supported schemes:
//   - memory:// and mem://
//   - badger:///path/to/db?node=worker-a
//   - etcd://127.0.0.1:2379?node=worker-a&prefix=app
//
// Supported query parameters:
//   - node: required unique node name for this cluster client
//   - prefix: backend key prefix for badger and etcd
//   - node_lease_ttl: node lease TTL, default 30s
//   - node_renew_interval: node lease renew interval, default 10s, minimum 10ms
//   - master_lease_ttl: master lease TTL, default follows node_lease_ttl
//   - master_renew_interval: master lease renew interval, default follows node_renew_interval, minimum 10ms
//   - master_history_limit: number of recent master transitions to retain, default 2000
//   - event_retention_count: number of recent watch events to retain, default 2000
//   - event_cleanup_interval: master-only watch event cleanup interval, default follows master_renew_interval, minimum 10ms
//   - admission_timeout: synchronous admission wait timeout, default 30s
//   - admission_retention_count: number of terminal admission requests to retain, default 2000
//   - admission_terminal_retention: minimum terminal admission request retention duration, default 10m
//   - watch_buffer_size: per-watch channel buffer size
//
// Example:
//
//	c, _ := NewClusterFromURL("badger:///var/lib/app/cluster?node=worker-a&prefix=control")
//	defer c.Close()
//
//	type JobSpec struct {
//		Owner string `json:"owner,omitempty" cluster:"required,index"`
//	}
//	type JobStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"index"`
//	}
//
//	jobs, _ := Define(c, TypedResourceDef[JobSpec, JobStatus]{
//		Resource:   "jobs",
//		APIVersion: "example.test/v1",
//		Kind:       "Job",
//		Namespaced: true,
//	})
//	billingJobs, _ := jobs.Namespace("billing")
//	_, _ = billingJobs.Create(ctx, "daily", JobSpec{Owner: "billing"}, CreateOptions{})
func NewClusterFromURL(raw string) (*Cluster, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	options, err := clusterOptionsFromURL(parsed)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(parsed.Scheme) {
	case "memory", "mem":
		return OpenMemory(options)
	case "badger":
		return OpenBadger(parsed.Path, options)
	case "etcd":
		client, err := NewEtcd(parsed)
		if err != nil {
			return nil, err
		}
		c, err := OpenEtcd(client, options)
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		return c, nil
	default:
		return nil, fmt.Errorf("%w: unsupported scheme %q", ErrInvalidConfig, parsed.Scheme)
	}
}

func clusterOptionsFromURL(parsed *url.URL) (Options, error) {
	query := parsed.Query()
	options := Options{
		Prefix:   query.Get("prefix"),
		NodeName: query.Get("node"),
	}
	if value := query.Get("node_lease_ttl"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid node_lease_ttl", ErrInvalidConfig)
		}
		options.NodeLeaseTTL = parsedValue
	}
	if value := query.Get("node_renew_interval"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue < minBackgroundInterval {
			return Options{}, fmt.Errorf("%w: invalid node_renew_interval", ErrInvalidConfig)
		}
		options.NodeRenewInterval = parsedValue
	}
	if value := query.Get("master_lease_ttl"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid master_lease_ttl", ErrInvalidConfig)
		}
		options.MasterLeaseTTL = parsedValue
	}
	if value := query.Get("master_renew_interval"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue < minBackgroundInterval {
			return Options{}, fmt.Errorf("%w: invalid master_renew_interval", ErrInvalidConfig)
		}
		options.MasterRenewInterval = parsedValue
	}
	if value := query.Get("master_history_limit"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid master_history_limit", ErrInvalidConfig)
		}
		options.MasterHistoryLimit = parsedValue
	}
	if value := query.Get("event_retention_count"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid event_retention_count", ErrInvalidConfig)
		}
		options.EventRetentionCount = parsedValue
	}
	if value := query.Get("event_cleanup_interval"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue < minBackgroundInterval {
			return Options{}, fmt.Errorf("%w: invalid event_cleanup_interval", ErrInvalidConfig)
		}
		options.EventCleanupInterval = parsedValue
	}
	if value := query.Get("admission_timeout"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid admission_timeout", ErrInvalidConfig)
		}
		options.AdmissionTimeout = parsedValue
	}
	if value := query.Get("admission_retention_count"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid admission_retention_count", ErrInvalidConfig)
		}
		options.AdmissionRetentionCount = parsedValue
	}
	if value := query.Get("admission_terminal_retention"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid admission_terminal_retention", ErrInvalidConfig)
		}
		options.AdmissionTerminalRetention = parsedValue
	}
	if value := query.Get("watch_buffer_size"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid watch_buffer_size", ErrInvalidConfig)
		}
		options.WatchBufferSize = parsedValue
	}
	return options, nil
}
