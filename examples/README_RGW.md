# RGW Backend Example

This example demonstrates using RGW (RADOS Gateway) as the object storage backend for LanceDB via the **WrappingObjectStore** mechanism.

## Overview

The RGW integration allows LanceDB to use Ceph's RADOS Gateway for all object storage operations. This is achieved through:

1. **WrappingObjectStore Mechanism**: Injects RGW backend at table operation level
2. **RGW SAL API**: Direct calls to Ceph SAL (Storage Abstraction Layer) C API
3. **Driver/DPP Context**: RGW driver and DPP pointers passed from RGW application

## Architecture

```
RGW Application
  ↓ (provides driver/dpp)
lancedb_create_rgw_wrapper(driver, dpp, bucket)
  ↓ (creates RGWObjectStore)
WrappingObjectStore callback
  ↓ (passed via write_params)
LanceDB Table Operations
  ↓ (all I/O through wrapper)
RGW SAL C API
  ↓
Ceph Storage
```

## Current Status

### ✅ What Works

- **WrappingObjectStore Integration**: Fully implemented and tested
- **RGW Wrapper Creation**: `lancedb_create_rgw_wrapper()` function ready
- **S3-Compatible Mode**: Can use S3 API with RGW endpoint

### 🚧 URL Scheme Limitation

**Current**: Must use `s3://` or `file://` URL with WrappingObjectStore
**Future**: Native `rgw://` URL support (requires LanceDB ObjectStoreRegistry changes)

The WrappingObjectStore mechanism works regardless of URL scheme - it replaces the default ObjectStore with RGW backend for all table operations.

## Building the Example

### Prerequisites

1. **Ceph Development Libraries** (required for linking):
   ```bash
   # Fedora/RHEL
   sudo dnf install ceph-devel librados-devel

   # Ubuntu/Debian
   sudo apt-get install libceph-dev librados-dev
   ```

2. **RGW Feature Enabled**:
   ```bash
   cd build
   cmake .. -DBUILD_EXAMPLES=ON  # Automatically enables rgw feature
   ```

### Build

```bash
cd build
make rgw
```

**Note**: If you encounter Ceph linking errors, you need to add Ceph library paths to your environment:

```bash
export LD_LIBRARY_PATH=/usr/lib64/ceph:$LD_LIBRARY_PATH
make rgw
```

## Running the Example

### Option 1: With S3-Compatible RGW Endpoint

This mode uses RGW's S3-compatible API:

```bash
./rgw <rgw_endpoint> <region> <access_key> <secret_key> <bucket>

# Example with local RGW
./rgw http://localhost:8000 us-east-1 testid testsecret my-bucket
```

**What it does**:
- Connects to LanceDB with `s3://bucket/lancedb-rgw-test` URL
- Uses WrappingObjectStore callback to inject RGW backend
- Creates table using RGW SAL API (via S3 compatibility layer)
- Lists and drops tables

### Option 2: In Real RGW Application

When integrating with an actual RGW application (e.g., in Ceph RGW):

```cpp
#include "lancedb.h"
#include "rgw_sal.h"

class RGWOpVectorOp : public RGWOp {
  void execute(optional_yield y) override {
    // Get RGW context
    rgw::sal::Driver* driver = store->get_driver();
    const DoutPrefixProvider* dpp = this;

    // Setup RGW config
    RGWConfig config = {
      .driver = static_cast<void*>(driver),
      .dpp = static_cast<const void*>(dpp),
      .bucket = "vectors-bucket"
    };

    // Connect to LanceDB
    const char* uri = "s3://vectors-bucket/lancedb";
    LanceDBConnectBuilder* builder = lancedb_connect(uri);
    // ... set storage options ...
    LanceDBConnection* db = lancedb_connect_builder_execute(builder);

    // Create table with RGW wrapper
    LanceDBCreateTableBuilder* tbl_builder =
        lancedb_connection_create_table_builder(db, "vectors", schema, nullptr);

    // Setup write options with RGW wrapper
    LanceDBWriteOptions write_opts;
    lancedb_write_options_defaults(&write_opts);

    LanceDBObjectStoreParams store_params;
    lancedb_object_store_params_defaults(&store_params);
    store_params.wrap_fn = wrap_with_rgw;
    store_params.wrap_user_data = &config;
    write_opts.store_params = &store_params;

    tbl_builder = lancedb_create_table_builder_write_options(tbl_builder, &write_opts);

    LanceDBTable* table = nullptr;
    lancedb_create_table_builder_execute(tbl_builder, &table, &error);

    // All table I/O now uses RGW SAL API!
  }
};
```

## Implementation Details

### Wrapper Callback

The `wrap_with_rgw()` callback is invoked by LanceDB when table operations need object storage:

```cpp
LanceDBObjectStore* wrap_with_rgw(
    const LanceDBObjectStore* original,
    const char* const* keys,
    const char* const* values,
    size_t count,
    void* user_data) {
  auto* cfg = static_cast<RGWConfig*>(user_data);

  // Create RGW wrapper with driver/dpp
  return lancedb_create_rgw_wrapper(
      cfg->driver,
      cfg->dpp,
      cfg->bucket);
}
```

### RGW Driver/DPP

In a real RGW application:
- **driver**: Comes from `env.driver` (RGW environment)
- **dpp**: Comes from request context (`this` in RGWOp subclass)

In the example:
- **driver**: Simulated placeholder pointer
- **dpp**: nullptr (allowed)

### Write Parameters

The wrapper is passed via `ObjectStoreParams` in write options:

```cpp
LanceDBObjectStoreParams store_params;
lancedb_object_store_params_defaults(&store_params);
store_params.wrap_fn = wrap_with_rgw;
store_params.wrap_user_data = &config;  // Contains driver/dpp

LanceDBWriteOptions write_opts;
write_opts.store_params = &store_params;

// Use in table creation
lancedb_create_table_builder_write_options(builder, &write_opts);
```

## Troubleshooting

### Linking Errors

If you see errors like:
```
undefined reference to `ceph::buffer::v15_2_0::ptr::release()'
```

**Solution**: Add Ceph library linking to CMakeLists.txt:

```cmake
target_link_libraries(rgw
  lancedb
  Threads::Threads
  ${ARROW_LIBRARIES}
  ceph-common  # Add this
  rados        # Add this
)
```

### Missing RGW Feature

If you see:
```
Error: RGW support not enabled. Rebuild with --features rgw
```

**Solution**: Rebuild with RGW feature:
```bash
cd build
cmake .. -DBUILD_EXAMPLES=ON
make rgw
```

### Driver Initialization

In the standalone example, driver/dpp are simulated placeholders. For actual RGW SAL API calls, you need:

1. Initialize Ceph RGW driver
2. Get DPP from request context
3. Pass real pointers to `lancedb_create_rgw_wrapper()`

## Future: Native rgw:// URL Support

To enable native `rgw://bucket/path` URLs, LanceDB would need:

1. Custom ObjectStoreRegistry support in ConnectBuilder
2. RGW provider registration before connection
3. URL scheme handler for `rgw://`

**Current Workaround**: Use `s3://` or `file://` URL + WrappingObjectStore

See `docs/RGW_INTEGRATION.md` for complete architecture details.

## Related Examples

- **s3_wrapper.cpp**: Shows S3 WrappingObjectStore pattern (similar approach)
- **s3.cpp**: Shows S3 with storage options (simpler, no wrapper)

## References

- Implementation: `src/rgw_provider.rs`
- Documentation: `docs/RGW_INTEGRATION.md`
- Training Material: `training-material/LANCEDB-C-FFI-INTEGRATION.md`
- RGW Object Store: `arrow-rs-object-store/src/rgw/`
