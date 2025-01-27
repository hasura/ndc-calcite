use tracing::Level;

#[derive(Debug, Copy, Clone)]
pub enum VersionTag {
    Version3,
    Version4,
    Version5,
}

/// Emit deprecation warning text if the version is deprecated.
#[tracing::instrument(skip(version), level=Level::INFO)]
pub fn deprecated_config_warning(version: VersionTag) -> Option<String> {
    match version {
		VersionTag::Version3 => Some(
          "Warning: ndc-postgres configuration version '3' is deprecated.
Consider upgrading to the latest version:
https://hasura.io/docs/3.0/connectors/postgresql/configuration-reference/#upgrading-the-configuration-format-version".to_string()
		),
		VersionTag::Version4 => Some(
          "Warning: ndc-postgres configuration version '4' is deprecated.
Consider upgrading to the latest version:
https://hasura.io/docs/3.0/connectors/postgresql/configuration-reference/#upgrading-the-configuration-format-version".to_string()
		),
		VersionTag::Version5 => None,
	}
}
