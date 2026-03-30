fn main() {
    let cargo_manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let manifest_path = cargo_manifest_dir.join("terminal-games.json");
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    terminal_games_manifest::validate_manifest_file(&manifest_path)
        .unwrap_or_else(|error| panic!("invalid {}: {error}", manifest_path.display()));
}
