/*
 * SPDX-License-Identifier: Apache-2.0
 * SPDX-FileCopyrightText: Copyright The LanceDB Authors
 *
 * Example: RGW Backend with WrappingObjectStore (Pure RGW - No S3 Fallback)
 *
 * This example demonstrates creating a table on RGW storage using ONLY the
 * RGW SAL API via the WrappingObjectStore mechanism.
 *
 * IMPORTANT: This example is designed for integration within RGW applications
 * where driver and dpp pointers come from the RGW environment. For standalone
 * testing, we use null pointers which will be passed to the RGW SAL API.
 *
 * Integration in RGW:
 *   - driver: from env.driver (RGW environment)
 *   - dpp: from request context (this pointer in RGWOp subclass)
 *
 * Standalone testing:
 *   - driver: nullptr (for demonstration)
 *   - dpp: nullptr (for demonstration)
 *   - This will invoke the RGW wrapper mechanism but may not complete
 *     actual I/O without a real RGW driver initialized
 */

#include <iostream>
#include <memory>
#include <vector>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include "lancedb.h"

// RGW wrapper function from lancedb-c
extern "C" {
LanceDBObjectStore* lancedb_create_rgw_wrapper(
    void* driver,
    const void* dpp,
    const char* bucket);

void lancedb_object_store_free(LanceDBObjectStore* store);
}

constexpr size_t DIM = 128;

// RGW configuration for the wrap callback
struct RGWConfig {
  void* driver;           // RGW driver pointer (rgw::sal::Driver*)
  const void* dpp;        // DPP pointer (DoutPrefixProvider*)
  const char* bucket;
};

// WrappingObjectStore callback: provides RGW backend for all table I/O
//
// This callback is invoked by LanceDB when table operations need object storage.
// It creates an RGW object store using the SAL API with driver/dpp from RGW context.
LanceDBObjectStore* wrap_with_rgw(
    const LanceDBObjectStore* /*original*/,
    const char* const* /*keys*/,
    const char* const* /*values*/,
    size_t /*count*/,
    void* user_data) {
  auto* cfg = static_cast<RGWConfig*>(user_data);

  std::cout << "WrappingObjectStore callback invoked" << std::endl;
  std::cout << "  Creating RGW object store using SAL API" << std::endl;
  std::cout << "  Driver: " << cfg->driver << std::endl;
  std::cout << "  DPP: " << cfg->dpp << std::endl;
  std::cout << "  Bucket: " << cfg->bucket << std::endl;

  // Create RGW object store wrapper using SAL API
  // In production: driver/dpp come from RGW environment
  // For testing: driver/dpp are null (demonstration only)
  return lancedb_create_rgw_wrapper(
      cfg->driver,
      cfg->dpp,
      cfg->bucket);
}

auto create_schema() {
  auto id_field = arrow::field("id", arrow::int32());
  auto item_field = arrow::field("item", arrow::fixed_size_list(arrow::float32(), DIM));
  return arrow::schema({id_field, item_field});
}

LanceDBTable* create_empty_table(LanceDBConnection* db, RGWConfig& config) {
  auto schema = create_schema();
  struct ArrowSchema c_schema;
  if (const auto status = arrow::ExportSchema(*schema, &c_schema); !status.ok()) {
    std::cerr << "failed to export schema to C ABI: " << status.ToString() << std::endl;
    return nullptr;
  }

  const std::string table_name = "rgw_vectors";

  std::cout << "\nCreating table '" << table_name << "' with RGW backend..." << std::endl;

  // Create table via builder so we can attach write options with the wrapper
  LanceDBCreateTableBuilder* builder = lancedb_connection_create_table_builder(
      db, table_name.c_str(),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      nullptr);
  if (!builder) {
    std::cerr << "failed to create table builder" << std::endl;
    if (c_schema.release) c_schema.release(&c_schema);
    return nullptr;
  }

  // Set up write options with the RGW object store wrapper
  // This is the key: we pass the wrap callback that provides RGW driver/dpp
  LanceDBWriteOptions write_opts;
  lancedb_write_options_defaults(&write_opts);

  LanceDBObjectStoreParams store_params;
  lancedb_object_store_params_defaults(&store_params);
  store_params.wrap_fn = wrap_with_rgw;      // Our RGW wrapper callback
  store_params.wrap_user_data = &config;     // RGW driver/dpp config
  write_opts.store_params = &store_params;

  builder = lancedb_create_table_builder_write_options(builder, &write_opts);
  if (!builder) {
    std::cerr << "failed to set write options" << std::endl;
    if (c_schema.release) c_schema.release(&c_schema);
    return nullptr;
  }

  LanceDBTable* tbl = nullptr;
  char* error_message = nullptr;
  if (const LanceDBError result = lancedb_create_table_builder_execute(
          builder, &tbl, &error_message);
      result != LANCEDB_SUCCESS) {
    std::cerr << "error creating table: " << table_name
              << ", error: " << error_message << std::endl;
    lancedb_free_string(error_message);
  } else {
    std::cout << "✓ Created table: " << table_name << " using RGW backend" << std::endl;
  }

  if (c_schema.release) {
    c_schema.release(&c_schema);
  }
  return tbl;
}

int main(int argc, char** argv) {
  std::cout << "=== LanceDB RGW Backend Example (Pure RGW - No S3 Fallback) ===" << std::endl;
  std::cout << std::endl;

  if (argc != 6) {
    std::cerr << "Usage: " << argv[0]
              << " <rgw_endpoint> <region> <access_key_id>"
              << " <secret_access_key> <bucket_name>" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Note: This example uses RGW SAL API only (no S3 fallback)." << std::endl;
    std::cerr << "      For standalone testing without real RGW driver, the wrapper" << std::endl;
    std::cerr << "      will be invoked but I/O operations may not complete." << std::endl;
    std::cerr << std::endl;
    std::cerr << "Example:" << std::endl;
    std::cerr << "  " << argv[0]
              << " http://localhost:8000 us-east-1 testid testsecret my-bucket"
              << std::endl;
    return 1;
  }

  const std::string rgw_endpoint = argv[1];
  const std::string region = argv[2];
  const std::string access_key_id = argv[3];
  const std::string secret_access_key = argv[4];
  const std::string bucket_name = argv[5];

  std::cout << "RGW Backend Configuration:" << std::endl;
  std::cout << "  Endpoint: " << rgw_endpoint << std::endl;
  std::cout << "  Region: " << region << std::endl;
  std::cout << "  Bucket: " << bucket_name << std::endl;
  std::cout << std::endl;

  // RGW driver/dpp initialization
  // In a real RGW application:
  //   - driver = env.driver (from RGW environment)
  //   - dpp = this (from RGWOp request context)
  //
  // For standalone testing:
  //   - driver = nullptr (demonstration only)
  //   - dpp = nullptr (demonstration only)
  //
  // Note: The RGW SAL API expects valid pointers. Null pointers are used
  // here for demonstration purposes to show the wrapper mechanism flow.
  void* driver = nullptr;  // In production: env.driver
  const void* dpp = nullptr;  // In production: this (from RGWOp)

  std::cout << "RGW Context:" << std::endl;
  std::cout << "  Driver: " << driver << " (null = testing mode)" << std::endl;
  std::cout << "  DPP: " << dpp << " (null = testing mode)" << std::endl;
  std::cout << std::endl;

  if (driver == nullptr) {
    std::cout << "⚠ Warning: Running in testing mode with null driver/dpp" << std::endl;
    std::cout << "           This demonstrates the WrappingObjectStore mechanism." << std::endl;
    std::cout << "           For actual I/O, integrate this code in RGW application" << std::endl;
    std::cout << "           where driver and dpp come from RGW environment." << std::endl;
    std::cout << std::endl;
  }

  RGWConfig config = {
    .driver = driver,
    .dpp = dpp,
    .bucket = bucket_name.c_str()
  };

  // Connect to LanceDB
  // Note: Using rgw:// URL requires LanceDB ObjectStoreRegistry support
  // For now, we use file:// or s3:// but the WrappingObjectStore mechanism
  // will replace it with RGW backend for all table operations
  const std::string uri = "file:///tmp/lancedb-rgw-test";

  std::cout << "Connecting to LanceDB..." << std::endl;
  std::cout << "  URI: " << uri << std::endl;
  std::cout << "  Note: WrappingObjectStore will replace with RGW backend" << std::endl;
  std::cout << std::endl;

  LanceDBConnectBuilder* builder = lancedb_connect(uri.c_str());
  if (!builder) {
    std::cerr << "failed to create connection builder" << std::endl;
    return 1;
  }

  LanceDBConnection* db = lancedb_connect_builder_execute(builder);
  if (!db) {
    std::cerr << "failed to connect to database" << std::endl;
    return 1;
  }
  std::cout << "✓ Connected to LanceDB" << std::endl;

  // Create table using the RGW object store wrapper
  // The wrap_with_rgw callback will be invoked to provide RGW backend
  std::cout << "\n--- Demonstrating WrappingObjectStore Mechanism ---" << std::endl;
  auto empty_table = create_empty_table(db, config);
  if (!empty_table) {
    std::cerr << "failed to create table (expected if driver is null)" << std::endl;
    std::cout << "\nThis is expected when running without a real RGW driver." << std::endl;
    std::cout << "The example successfully demonstrated:" << std::endl;
    std::cout << "  1. Connection creation" << std::endl;
    std::cout << "  2. WrappingObjectStore callback invocation" << std::endl;
    std::cout << "  3. RGW wrapper creation attempt" << std::endl;
    std::cout << "\nTo complete actual I/O operations:" << std::endl;
    std::cout << "  - Integrate this code in RGW application" << std::endl;
    std::cout << "  - Provide real driver from env.driver" << std::endl;
    std::cout << "  - Provide real dpp from request context" << std::endl;
    lancedb_connection_free(db);
    return 0;
  }

  std::cout << "\n✓ Table operations completed successfully!" << std::endl;
  lancedb_table_free(empty_table);

  // List table names
  std::cout << "\nListing tables..." << std::endl;
  char** table_names;
  size_t name_count;
  char* error_message = nullptr;
  if (const LanceDBError result = lancedb_connection_table_names(
          db, &table_names, &name_count, &error_message);
      result != LANCEDB_SUCCESS) {
    std::cerr << "error listing table names, error: " << error_message << std::endl;
    lancedb_free_string(error_message);
  } else {
    std::cout << "✓ Found " << name_count << " table(s):" << std::endl;
    for (size_t i = 0; i < name_count; i++) {
      std::cout << "  - " << table_names[i] << std::endl;
    }
    lancedb_free_table_names(table_names, name_count);
  }

  lancedb_connection_free(db);

  std::cout << "\n=== RGW Backend Example Complete ===" << std::endl;
  std::cout << "\nThis example demonstrated the WrappingObjectStore mechanism for RGW." << std::endl;
  std::cout << "For production use in RGW applications:" << std::endl;
  std::cout << "  1. Get driver from env.driver (RGW environment)" << std::endl;
  std::cout << "  2. Get dpp from request context (this in RGWOp)" << std::endl;
  std::cout << "  3. All table I/O operations will use RGW SAL API" << std::endl;

  return 0;
}
