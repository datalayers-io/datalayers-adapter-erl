//! Link-time stand-ins for the NIF API used by Rustler's Linux test binaries.
//!
//! Rustler 0.39 links `enif_*` symbols directly into Rust test executables,
//! but those symbols only exist while the NIF is loaded by the BEAM. The unit
//! tests do not invoke these functions; Common Test verifies the real NIF.

macro_rules! nif_stubs {
    ($($name:ident),* $(,)?) => {
        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn $name() -> usize {
                panic!("BEAM NIF API is unavailable in Rust unit tests")
            }
        )*
    };
}

nif_stubs!(
    enif_alloc_env,
    enif_alloc_resource,
    enif_clear_env,
    enif_free_env,
    enif_get_atom,
    enif_get_atom_length,
    enif_get_double,
    enif_get_int,
    enif_get_list_cell,
    enif_get_local_pid,
    enif_get_long,
    enif_get_resource,
    enif_get_tuple,
    enif_get_uint,
    enif_get_ulong,
    enif_inspect_binary,
    enif_is_atom,
    enif_is_empty_list,
    enif_is_exception,
    enif_is_identical,
    enif_is_list,
    enif_is_map,
    enif_is_ref,
    enif_keep_resource,
    enif_make_atom_len,
    enif_make_badarg,
    enif_make_copy,
    enif_make_list_from_array,
    enif_make_long,
    enif_make_new_binary,
    enif_make_resource,
    enif_make_tuple_from_array,
    enif_map_iterator_create,
    enif_map_iterator_destroy,
    enif_map_iterator_get_pair,
    enif_map_iterator_next,
    enif_map_iterator_prev,
    enif_raise_exception,
    enif_release_resource,
    enif_schedule_nif,
    enif_send,
    enif_thread_type,
);
