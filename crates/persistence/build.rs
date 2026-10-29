fn main() {
    // `sqlx::migrate!` embeds files at compile time. Watching the shared
    // migrations directory also makes Cargo rebuild when a new SQL file is added.
    println!("cargo:rerun-if-changed=../../migrations");
}
