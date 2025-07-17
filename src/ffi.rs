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
    env_out: *mut *mut Env,
) -> ErrorCode {
    if env_out.is_null() { return ErrorCode::NullPtr; }

    let cstr = if root.is_null() {
        return ErrorCode::NullPtr;
    } else {
        unsafe { std::ffi::CStr::from_ptr(root) }
    };

    match Env::new(
        std::path::PathBuf::from(cstr.to_string_lossy().into_owned()),
        if max_topics == 0 { None } else { Some(max_topics) },
        if map_size == 0 { None } else { Some(map_size) },
    ) {
        Ok(env) => {
            let boxed = Box::new(env);
            unsafe { *env_out = Box::into_raw(boxed); }
            ErrorCode::Ok
        }
        Err(_) => {
            unsafe { *env_out = std::ptr::null_mut(); }
            ErrorCode::CreationFailed
        }
    }
}
