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

#[repr(C)]
pub struct CItem {
    pub ts: u64,
    pub data: *mut u8,
    pub len: usize,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_consumer_pop(consumer: *mut Consumer) -> *mut CItem {
    let consumer: &mut Consumer = unsafe { &mut *consumer };
    match consumer.pop_front() {
        Ok(Some(item)) => {
            let len = item.data.len();
            let data = item.data.into_boxed_slice();
            Box::into_raw(Box::new(CItem {
                ts: item.ts,
                data: Box::into_raw(data) as *mut u8,
                len
            }))
        }
        Ok(None) => std::ptr::null_mut(),
        Err(e) => {
            eprintln!("queue_env_new error: {:?}", e);
            std::ptr::null_mut()        
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_item_free(ptr: *mut CItem) {
    if ptr.is_null() {
        return;
    }

    let citem = unsafe { Box::from_raw(ptr) };
    if !citem.data.is_null() && citem.len > 0 {
        let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(citem.data, citem.len)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_consumer_pop_n(
    consumer: *mut Consumer,
    n: u64,
    out_items: *mut *mut CItem,
    out_count: *mut libc::size_t,
) -> i32 {
    if consumer.is_null() || out_items.is_null() || out_count.is_null() {
        return 1; // Invalid argument
    }

    let consumer = unsafe { &mut *consumer };

    match consumer.pop_front_n(n) {
        Ok(items) => {
            let count = items.len();
            let mut citems: Vec<CItem> = Vec::with_capacity(count);

            for item in items {
                let len = item.data.len();
                let data_ptr = Box::into_raw(item.data.into_boxed_slice()) as *mut u8;
                citems.push(CItem {
                    ts: item.ts,
                    data: data_ptr,
                    len,
                });
            }

            let raw_ptr = citems.as_mut_ptr();
            std::mem::forget(citems);
            unsafe {
                *out_items = raw_ptr;
                *out_count = count;
            }

            0 // success
        }
        Err(e) => {
            eprintln!("queue_env_new error: {:?}", e);
            unsafe {
                *out_items = std::ptr::null_mut();
                *out_count = 0;
            }
            -1 // error code
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn queue_items_free(items: *mut CItem, count: libc::size_t) {
    if items.is_null() { return; }

    let slice = unsafe { std::slice::from_raw_parts_mut(items, count) };
    for item in slice {
        if !item.data.is_null() && item.len > 0 {
            let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(item.data, item.len)) };
        }
    }

    let _ = unsafe { Vec::from_raw_parts(items, count, count) }; // 释放 CItem 本体
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