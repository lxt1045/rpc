package socks

import "expvar"

var (
	// Metrics are process-level counters exposed through expvar on the
	// configured debug/metrics HTTP endpoint.
	MetricsSessions     = expvar.NewInt("socks_trunk_sessions")
	MetricsAuthFailures = expvar.NewInt("socks_trunk_auth_failures")
	MetricsTrunkConns   = expvar.NewInt("socks_trunk_trunk_conns")
	MetricsActiveConns  = expvar.NewInt("socks_trunk_active_conns")
)

// Metrics helpers are called from session/trunk lifecycle points so an
// operator can observe live usage without scraping the whole pprof page.
func metricSessionsAdd(delta int64) { MetricsSessions.Add(delta) }
func metricAuthFailureInc()         { MetricsAuthFailures.Add(1) }
func metricTrunkConnsAdd(delta int64) {
	MetricsTrunkConns.Add(delta)
	if MetricsTrunkConns.Value() < 0 {
		MetricsTrunkConns.Set(0)
	}
}
func metricActiveConnsAdd(delta int64) {
	MetricsActiveConns.Add(delta)
	if MetricsActiveConns.Value() < 0 {
		MetricsActiveConns.Set(0)
	}
}
