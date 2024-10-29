//! Metrics setup and update for our connector.

use prometheus::{IntGauge, Registry};

use crate::version::VersionTag;

/// The collection of configuration-related metrics exposed through the `/metrics` endpoint.
#[derive(Debug, Clone)]
pub struct Metrics {
    configuration_version_3: IntGauge,
    configuration_version_4: IntGauge,
    configuration_version_5: IntGauge,
}
