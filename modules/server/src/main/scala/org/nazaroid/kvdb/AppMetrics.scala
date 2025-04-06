package com.uzumdata.cc.api

import io.prometheus.client.{Counter, Gauge}

class AppMetrics {
  val profilesTableSwitchGauge: Gauge = Gauge
    .build()
    .name("cc_profiles_table_switch")
    .labelNames("table")
    .help("indicates when active profiles table switched").register()

  val userCounter: Counter = Counter
    .build()
    .name("auth_status")
    .help("Requests auth status counter, labeled with user login.")
    .labelNames("login", "status", "reason")
    .register()

  val customerProfilesClassifiedRequests: Counter = Counter
    .build()
    .name("customer_profiles_classified_requests")
    .help("Requests counter, classified by selector type and labeled with user login.")
    .labelNames("login", "classifier")
    .register()
}
