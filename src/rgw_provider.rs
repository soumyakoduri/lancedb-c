// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! RGW object store provider for lancedb-c
//!
//! This module provides FFI functions to create RGW-backed object stores
//! that can be used with LanceDB via the WrappingObjectStore mechanism.
//!
//! # Architecture
//!
//! Following the pattern from training-material/LANCEDB-C-FFI-INTEGRATION.md:
//! 1. RGWObjectStore is created with driver/dpp from arrow-rs-object-store
//! 2. Wrapped in RGWWrappingStore that implements WrappingObjectStore
//! 3. Passed via ObjectStoreParams in write_params/read_params
//! 4. NOT used during connection creation

use std::ffi::{c_void, CStr, c_char};
use std::sync::Arc;

use lance::io::WrappingObjectStore;
use object_store::ObjectStore;

/// Wrapper that implements WrappingObjectStore for RGW
///
/// This allows RGWObjectStore to be used with LanceDB's write_params/read_params
/// mechanism without modifying LanceDB APIs.
#[derive(Debug)]
pub struct RGWWrappingStore {
    inner: Arc<dyn ObjectStore>,
}

impl RGWWrappingStore {
    /// Create a new RGW wrapping store
    ///
    /// # Safety
    /// The inner ObjectStore must be a valid RGWObjectStore
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { inner: store }
    }
}

impl WrappingObjectStore for RGWWrappingStore {
    fn wrap(
        &self,
        _original: Arc<dyn ObjectStore>,
        _options: Option<&std::collections::HashMap<String, String>>,
    ) -> Arc<dyn ObjectStore> {
        // Return our RGW ObjectStore instead of the default/original
        // This ensures all I/O goes through RGW SAL API
        self.inner.clone()
    }
}

/// Create an RGW object store wrapper suitable for use with LanceDB
///
/// This creates an RGWObjectStore from arrow-rs and wraps it in a
/// WrappingObjectStore implementation that can be passed via write_params/read_params.
///
/// # Safety
/// - `driver` must be a valid pointer to an initialized RGW Driver
/// - `dpp` can be null or must be a valid pointer to a DoutPrefixProvider
/// - `bucket` must be a valid null-terminated C string
/// - Both driver and dpp pointers must remain valid for the lifetime of the object store
/// - The driver must be thread-safe
///
/// # Returns
/// - Pointer to LanceDBObjectStore (wrapping RGWWrappingStore) on success
/// - Null pointer on failure
///
/// # Example
///
/// ```c
/// // In C/C++ code with initialized RGW driver:
/// LanceDBObjectStore* rgw_wrapper = lancedb_create_rgw_wrapper(
///     driver_ptr,
///     dpp_ptr,
///     "my-bucket"
/// );
/// if (rgw_wrapper != NULL) {
///     // Use in write_params via wrapping_object_store callback
/// }
/// ```
#[cfg(feature = "rgw")]
#[no_mangle]
pub unsafe extern "C" fn lancedb_create_rgw_wrapper(
    driver: *mut c_void,
    dpp: *const c_void,
    bucket: *const c_char,
) -> *mut crate::types::LanceDBObjectStore {
    use object_store::rgw::RGWObjectStoreBuilder;

    if driver.is_null() {
        eprintln!("Error: RGW driver pointer is null");
        return std::ptr::null_mut();
    }

    if bucket.is_null() {
        eprintln!("Error: bucket name is null");
        return std::ptr::null_mut();
    }

    let bucket_str = match CStr::from_ptr(bucket).to_str() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: Invalid bucket name: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Create RGW object store using builder from arrow-rs
    let rgw_store = match RGWObjectStoreBuilder::new(driver, dpp)
        .with_bucket(bucket_str.to_string())
        .build()
    {
        Ok(store) => store,
        Err(e) => {
            eprintln!("Error creating RGW object store: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Wrap in Arc<dyn ObjectStore>
    let store_arc = Arc::new(rgw_store) as Arc<dyn ObjectStore>;

    // Wrap in LanceDBObjectStore
    let wrapped = crate::types::LanceDBObjectStore {
        inner: store_arc,
    };

    Box::into_raw(Box::new(wrapped))
}

#[cfg(not(feature = "rgw"))]
#[no_mangle]
pub unsafe extern "C" fn lancedb_create_rgw_wrapper(
    _driver: *mut c_void,
    _dpp: *const c_void,
    _bucket: *const c_char,
) -> *mut crate::types::LanceDBObjectStore {
    eprintln!("Error: RGW support not enabled. Rebuild with --features rgw");
    std::ptr::null_mut()
}

/// Helper to create a WrappingObjectStore from an RGW object store
///
/// This is used internally by the wrapping_object_store callback mechanism.
///
/// # Safety
/// - `rgw_store` must be a valid LanceDBObjectStore created by lancedb_create_rgw_wrapper
///
/// # Returns
/// - Arc<dyn WrappingObjectStore> that can be used with ObjectStoreParams
#[cfg(feature = "rgw")]
pub unsafe fn create_rgw_wrapping_store(
    rgw_store: *const crate::types::LanceDBObjectStore,
) -> Arc<dyn WrappingObjectStore> {
    if rgw_store.is_null() {
        panic!("RGW store pointer is null");
    }

    let store_ref = &*rgw_store;
    let wrapper = RGWWrappingStore::new(store_ref.inner.clone());
    Arc::new(wrapper) as Arc<dyn WrappingObjectStore>
}
