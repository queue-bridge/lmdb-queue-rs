use super::env::Env;
use super::topic::{Producer, Comsumer};

#[repr(C)]
pub enum ErrorCode {
    Ok = 0,
    NullPtr = 1,
    CreationFailed = 2,
}

#[unsafe(no_mangle)]
pub unsafe  extern "C" fn queue_env_new(
    root: *const libc::c_char,
    max_topics: libc::c_uint,
    map_size: libc::size_t,
) -> *mut Env {
    let root = unsafe {
        assert!(!root.is_null());
        std::ffi::CStr::from_ptr(root)
    };
    
    match Env::new(
        std::path::PathBuf::from(root.to_string_lossy().into_owned()),
        if max_topics == 0 { None } else { Some(max_topics) },
        if map_size == 0 { None } else { Some(map_size) },
    ) {
        Ok(env) => Box::into_raw(Box::new(env)),
        Err(e) => {
            eprintln!("queue_env_new error: {:?}", e);
            std::ptr::null_mut()
        }
    }
}
