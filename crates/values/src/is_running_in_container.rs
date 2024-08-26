use std::env;
use std::path::Path;
use tracing::{Level};

/// Checks if the code is running inside a container.
///
/// This function checks for the existence of the `/.dockerenv` file in the filesystem,
/// which is commonly used to indicate that the code is running inside a Docker container.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// fn is_running_in_container() -> bool {
///     Path::new("/.dockerenv").exists()
/// }
///
/// assert_eq!(is_running_in_container(), false);
/// ```
///
/// # Returns
///
/// Returns `true` if the code is running inside a container, `false` otherwise.
#[tracing::instrument(skip(), level=Level::INFO)]
pub fn is_running_in_container() -> bool {
    Path::new("/.dockerenv").exists() || env::var("KUBERNETES_SERVICE_HOST").is_ok()
}