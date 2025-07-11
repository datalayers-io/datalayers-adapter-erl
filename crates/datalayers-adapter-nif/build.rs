use std::{env, fs::File, io::Write, path::Path};

fn main() {
    // Directory contain this build-script
    // $PROJ_ROOT/crates/datalayers-adapter-nif/
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    // $PROJ_ROOT/
    let workspace_dir = Path::new(&manifest_dir).parent().unwrap().parent().unwrap();

    // Host triple (arch of machine doing to build, not necessarily the arch we're building for)
    let host_triple = env::var("HOST").unwrap();

    // Target triple (arch we're building for, not necessarily the arch we're building on)
    let target_triple = env::var("TARGET").unwrap();

    // debug or release
    let profile = env::var("PROFILE").unwrap();

    // We use target OS to determine if extension is `.so`, `.dll`, or `.dylib`
    // FIXME: the target file name setting here not work
    let file_name = match env::var("CARGO_CFG_TARGET_OS").unwrap().as_str() {
        "windows" => "datalayers_nif.dll",
        "macos" | "ios" => "libdatalayers_nif.dylib",
        _ => "libdatalayers_nif.so",
    };

    let mut libpath = workspace_dir.join("target");
    if host_triple != target_triple {
        libpath = libpath.join(&target_triple);
    }
    libpath = libpath.join(&profile).join(file_name);

    // Create file in `here` and write the path to the directory of
    // $PROJ_ROOT/libpath
    let libpath_file_path = Path::new(&workspace_dir).join("libpath");
    let mut libpath_file = File::create(libpath_file_path).unwrap();
    write!(libpath_file, "{}", libpath.to_str().unwrap()).unwrap();
}
