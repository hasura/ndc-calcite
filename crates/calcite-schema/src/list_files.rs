use std::path::Path;
use tracing::debug;

#[tracing::instrument(skip())]
pub fn list_files_in_directory(dir: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    debug!("Inspecting directory: {}", dir.display());
    let attempts = 1;
    let seconds = 1;

    for attempt in 0..attempts {
        let mut file_list = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                debug!("Found directory: {}", path.display());
                // If path is a directory, call the function recursively
                let mut more = list_files_in_directory(&path)?;
                file_list.append(&mut more);
            } else if path.is_file() {
                debug!("Found file: {}", path.display());
                // Convert PathBuf to string and add to the list
                let path_string = path.to_str().unwrap().to_string();
                file_list.push(path_string);
            }
        }

        if !file_list.is_empty() {
            return Ok(file_list);
        }

        // if no files were found, sleep for 5 seconds before trying again
        if attempt < attempts - 1 {
            eprintln!("No files found on attempt {}. Retrying after {} seconds...", attempt + 1, seconds);
            std::thread::sleep(std::time::Duration::from_secs(seconds));
        }
    }

    // If no files found after all retries, return empty Vec
    Ok(Vec::new())
}
