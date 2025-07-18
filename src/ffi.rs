use super::env::Env;
use super::topic::{Producer, Consumer};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_env_new(
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_env_free(env: *mut Env) {
    if !env.is_null() {
        unsafe { drop(Box::from_raw(env)); }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_consumer_new(
    env: *mut Env,
    name: *const libc::c_char,
    chunks_to_keep: u64,
) -> *mut Consumer<'static> {
    assert!(!env.is_null());
    assert!(!name.is_null());

    let env: &mut Env = unsafe { &mut *env };
    let name = unsafe { std::ffi::CStr::from_ptr(name) };
    
    match env.consumer(
        &name.to_string_lossy(),
        if chunks_to_keep == 0 { None } else { Some(chunks_to_keep) },
    ) {
        Ok(consumer) => Box::into_raw(Box::new(consumer)),
        Err(e) => {
            eprintln!("queue_env_new error: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_consumer_free(consumer: *mut Consumer) {
    if !consumer.is_null() {
        unsafe { drop(Box::from_raw(consumer)); }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_producer_new(
    env: *mut Env,
    name: *const libc::c_char,
    chunk_size: u64,
) -> *mut Producer<'static> {
    assert!(!env.is_null());
    assert!(!name.is_null());

    let env: &mut Env = unsafe { &mut *env };
    let name = unsafe { std::ffi::CStr::from_ptr(name) };
    
    match env.producer(
        &name.to_string_lossy(),
        if chunk_size == 0 { None } else { Some(chunk_size) },
    ) {
        Ok(producer) => Box::into_raw(Box::new(producer)),
        Err(e) => {
            eprintln!("queue_env_new error: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_producer_free(producer: *mut Producer) {
    if !producer.is_null() {
        unsafe { drop(Box::from_raw(producer)); }
    }
}