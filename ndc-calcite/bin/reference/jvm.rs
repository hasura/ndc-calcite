use std::{env, fs};

use jni::{InitArgsBuilder, JavaVM, JNIVersion};

pub fn create_jvm() -> JavaVM {
    let folder_path = env::var("JAR_DEPENDENCY_FOLDER").unwrap_or_default(); // Replace with your actual folder path
    let mut jar_paths: Vec<String> = Vec::new();
    if !folder_path.is_empty() {
        if let Ok(entries) = fs::read_dir(folder_path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() && path.extension().map(|ext| ext == "jar").unwrap_or(false) {
                        jar_paths.push(path.to_string_lossy().to_string());
                    }
                }
            }
        }
    }
    let jar_name = env::var("CALCITE_JAR").unwrap_or_default();
    if !jar_name.is_empty() {
        jar_paths.push(jar_name);
    }
    let expanded_paths: String = jar_paths.join(":");
    let mut jvm_args = InitArgsBuilder::new().version(JNIVersion::V8);
    let log4j2_config_file = env::var("log4j2_config_file").unwrap_or_default();
    if !log4j2_config_file.is_empty() {
        jvm_args = jvm_args.option(format!("-Dlog4j.configurationFile={}", log4j2_config_file));
    }
    if !expanded_paths.is_empty() {
        jvm_args = jvm_args.option(["-Djava.class.path=", &expanded_paths].join(""))
    }
    let jvm_args = jvm_args.build().unwrap();
    JavaVM::new(jvm_args).unwrap()
}
