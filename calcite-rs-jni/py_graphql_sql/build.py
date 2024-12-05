import shutil
import os
import subprocess

# Define the source and destination paths
source_path = os.path.join('..', 'jdbc', 'target', 'graphql-jdbc-driver-1.0.0-jar-with-dependencies.jar')
destination_dir = os.path.join('.', 'py_graphql_sql', 'jars')


def copy_jar_file():
    # Make sure the destination directory exists
    os.makedirs(destination_dir, exist_ok=True)
    # Copy the file to the destination directory
    shutil.copy(source_path, destination_dir)
    print(f"File has been copied from {source_path} to {destination_dir}")


def clean_metadata_files():
    try:
        # Run dot_clean to remove Mac-specific metadata files
        subprocess.run(["dot_clean", "."], check=True)
        print("dot_clean executed successfully.")
    except FileNotFoundError:
        print("dot_clean command not found. Skipping this step.")


def install_dependencies():
    try:
        # Run poetry install
        subprocess.run(["poetry", "install"], check=True)
        print("Dependencies installed using Poetry.")
    except subprocess.CalledProcessError:
        print("Failed to install using Poetry. Trying pip as a fallback.")
        # If poetry fails, you can try pip install as an alternative
        # subprocess.run(["pip", "install", "."], check=True)


def main():
    copy_jar_file()
    clean_metadata_files()
    install_dependencies()


if __name__ == "__main__":
    main()
