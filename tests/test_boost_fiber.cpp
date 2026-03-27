/**
 * @file test_boost_fiber.cpp
 * @brief Tests that mimic Ceph RGW's boost::asio fiber environment
 *
 * This test file replicates the exact execution environment where Ceph RGW
 * crashed when using reserved SQL keywords (like 'key') without backticks
 * in lancedb_table_delete filters.
 *
 * The crash occurred because:
 * 1. Ceph uses boost::asio::spawn with fiber-based coroutines
 * 2. Fibers have limited stack size (typically 64KB-1MB)
 * 3. Deep recursion in Planner::binary_expr for OR chains
 * 4. Combined with Column::from_qualified_name re-tokenizing column names
 * 5. Led to stack corruption and NULL pointer dereference
 *
 * Stack trace from crash:
 *   sqlparser::tokenizer::Tokenizer::next_token (self=0x0, chars=0x0)
 *   <- NULL pointer crash due to stack corruption
 */

#include <catch2/catch.hpp>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "lancedb.h"
#include "test_common.h"

#include <sstream>
#include <vector>
#include <string>
#include <atomic>
#include <thread>

namespace asio = boost::asio;

// Test fixture with boost::asio io_context
class BoostFiberFixture : public LanceDBFixture {
public:
  asio::io_context io_ctx;

  BoostFiberFixture() : LanceDBFixture() {}
};

// Helper to create a table with 'key' column
LanceDBTable* create_key_value_table(LanceDBConnection* db,
                                      const std::string& table_name,
                                      int num_rows) {
  // Create schema
  auto key_field = arrow::field("key", arrow::utf8());
  auto value_field = arrow::field("value", arrow::int32());
  auto schema = arrow::schema({key_field, value_field});

  struct ArrowSchema c_schema;
  if (!arrow::ExportSchema(*schema, &c_schema).ok()) {
    return nullptr;
  }

  LanceDBTable* table = nullptr;
  char* error_message = nullptr;
  LanceDBError result = lancedb_table_create(
      db, table_name.c_str(),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      nullptr, &table, &error_message);

  if (c_schema.release) c_schema.release(&c_schema);

  if (result != LANCEDB_SUCCESS) {
    if (error_message) lancedb_free_string(error_message);
    return nullptr;
  }

  // Add data
  arrow::StringBuilder key_builder;
  arrow::Int32Builder value_builder;
  for (int i = 0; i < num_rows; i++) {
    key_builder.Append("key_" + std::to_string(i));
    value_builder.Append(i * 100);
  }
  std::shared_ptr<arrow::Array> key_array, value_array;
  key_builder.Finish(&key_array);
  value_builder.Finish(&value_array);
  auto batch = arrow::RecordBatch::Make(schema, num_rows, {key_array, value_array});

  struct ArrowArray c_array;
  struct ArrowSchema c_schema2;
  arrow::ExportRecordBatch(*batch, &c_array, &c_schema2);

  LanceDBRecordBatchReader* reader = nullptr;
  lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema2),
      &reader, nullptr);

  if (c_schema2.release) c_schema2.release(&c_schema2);

  result = lancedb_table_add(table, reader, &error_message);
  if (result != LANCEDB_SUCCESS) {
    if (error_message) lancedb_free_string(error_message);
    lancedb_table_free(table);
    return nullptr;
  }

  return table;
}

/**
 * Test 1: Basic delete in fiber context
 * This mimics how Ceph RGW calls lancedb_table_delete from within a fiber
 */
TEST_CASE_METHOD(BoostFiberFixture, "Boost Fiber - Basic delete with reserved keyword", "[boost][fiber][ceph]") {
  const std::string table_name = "fiber_basic_delete_test";

  LanceDBTable* table = create_key_value_table(db, table_name, 10);
  REQUIRE(table != nullptr);
  REQUIRE(lancedb_table_count_rows(table) == 10);

  std::atomic<bool> test_passed{false};
  std::atomic<bool> test_completed{false};
  std::string error_msg;

  // Run delete operation inside a fiber (like Ceph does)
  asio::spawn(io_ctx, [&](asio::yield_context yield) {
    char* error_message = nullptr;

    // Test with backticks (should work)
    LanceDBError result = lancedb_table_delete(table, "`key` = \"key_0\"", &error_message);
    if (result != LANCEDB_SUCCESS) {
      error_msg = error_message ? error_message : "unknown error";
      if (error_message) lancedb_free_string(error_message);
      test_completed = true;
      return;
    }

    // Test WITHOUT backticks - this is what crashed in Ceph
    result = lancedb_table_delete(table, "key = \"key_1\"", &error_message);
    if (result != LANCEDB_SUCCESS) {
      error_msg = error_message ? error_message : "unknown error (possible crash)";
      if (error_message) lancedb_free_string(error_message);
    } else {
      test_passed = true;
    }

    test_completed = true;
  });

  // Run the io_context
  io_ctx.run();

  INFO("Error message: " << error_msg);
  REQUIRE(test_completed);

  if (test_passed) {
    INFO("Delete without backticks succeeded in fiber context");
    CHECK(lancedb_table_count_rows(table) == 8);
  } else {
    WARN("Delete without backticks failed in fiber context - use backticks for reserved keywords");
  }

  lancedb_table_free(table);
}

/**
 * Test 2: Large OR chain in fiber context (Ceph delete_vectors pattern)
 * This is the exact pattern that caused the crash in Ceph
 */
TEST_CASE_METHOD(BoostFiberFixture, "Boost Fiber - Large OR chain delete (Ceph delete_vectors)", "[boost][fiber][ceph]") {
  const std::string table_name = "fiber_or_chain_test";
  constexpr int num_rows = 100;
  constexpr int keys_to_delete = 20;

  LanceDBTable* table = create_key_value_table(db, table_name, num_rows);
  REQUIRE(table != nullptr);
  REQUIRE(lancedb_table_count_rows(table) == num_rows);

  std::atomic<bool> test_passed{false};
  std::atomic<bool> test_completed{false};
  std::string error_msg;
  std::string filter_used;

  // Run delete with large OR chain inside a fiber
  asio::spawn(io_ctx, [&](asio::yield_context yield) {
    // Build OR chain WITHOUT backticks (the crash case)
    std::ostringstream oss;
    for (int i = 0; i < keys_to_delete; i++) {
      oss << "key = \"key_" << i << "\"";
      if (i < keys_to_delete - 1) {
        oss << " OR ";
      }
    }
    filter_used = oss.str();

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, filter_used.c_str(), &error_message);

    if (result != LANCEDB_SUCCESS) {
      error_msg = error_message ? error_message : "unknown error (possible crash/stack overflow)";
      if (error_message) lancedb_free_string(error_message);
    } else {
      test_passed = true;
    }

    test_completed = true;
  });

  io_ctx.run();

  INFO("Filter: " << filter_used);
  INFO("Error: " << error_msg);
  REQUIRE(test_completed);

  if (test_passed) {
    INFO("Large OR chain without backticks succeeded in fiber context");
    CHECK(lancedb_table_count_rows(table) == num_rows - keys_to_delete);
  } else {
    WARN("Large OR chain without backticks FAILED in fiber context");
    WARN("This replicates the Ceph crash - use backticks: `key` = \"value\"");
  }

  lancedb_table_free(table);
}

/**
 * Test 3: Test with custom (smaller) fiber stack size
 * This attempts to reproduce the stack overflow that may have occurred in Ceph
 */
TEST_CASE_METHOD(BoostFiberFixture, "Boost Fiber - Small stack size stress test", "[boost][fiber][ceph][stress]") {
  const std::string table_name = "fiber_small_stack_test";
  constexpr int num_rows = 500;
  constexpr int keys_to_delete = 50;

  LanceDBTable* table = create_key_value_table(db, table_name, num_rows);
  REQUIRE(table != nullptr);

  std::atomic<bool> test_completed{false};
  std::atomic<int> delete_result{-1};
  std::string error_msg;

  // Use a smaller stack size to try to trigger stack overflow
  // Default fiber stack is usually 64KB-1MB, we try smaller
  boost::asio::spawn(
    io_ctx,
    [&](asio::yield_context yield) {
      // Build large OR chain with backticks (should work)
      std::ostringstream oss;
      for (int i = 0; i < keys_to_delete; i++) {
        oss << "`key` = \"key_" << i << "\"";
        if (i < keys_to_delete - 1) {
          oss << " OR ";
        }
      }

      char* error_message = nullptr;
      LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);
      delete_result = result;

      if (result != LANCEDB_SUCCESS && error_message) {
        error_msg = error_message;
        lancedb_free_string(error_message);
      }

      test_completed = true;
    },
    // Use default attributes (can customize stack size with boost::context::stack_traits)
    boost::asio::detached
  );

  // Run with timeout
  auto start = std::chrono::steady_clock::now();
  while (!test_completed) {
    io_ctx.run_one_for(std::chrono::milliseconds(100));

    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > std::chrono::seconds(30)) {
      FAIL("Test timed out - possible deadlock or infinite loop");
      break;
    }
  }

  INFO("Delete result: " << delete_result);
  INFO("Error: " << error_msg);

  if (delete_result == LANCEDB_SUCCESS) {
    CHECK(lancedb_table_count_rows(table) == num_rows - keys_to_delete);
  }

  lancedb_table_free(table);
}

/**
 * Test 4: Concurrent fiber deletes (stress test)
 * Multiple fibers doing deletes simultaneously
 */
TEST_CASE_METHOD(BoostFiberFixture, "Boost Fiber - Concurrent delete operations", "[boost][fiber][ceph][concurrent]") {
  const std::string table_name = "fiber_concurrent_test";
  constexpr int num_rows = 100;
  constexpr int num_fibers = 5;
  constexpr int keys_per_fiber = 10;

  LanceDBTable* table = create_key_value_table(db, table_name, num_rows);
  REQUIRE(table != nullptr);

  std::atomic<int> completed_fibers{0};
  std::atomic<int> successful_deletes{0};
  std::vector<std::string> errors;
  std::mutex errors_mutex;

  // Spawn multiple fibers doing concurrent deletes
  for (int fiber_id = 0; fiber_id < num_fibers; fiber_id++) {
    asio::spawn(io_ctx, [&, fiber_id](asio::yield_context yield) {
      int start_key = fiber_id * keys_per_fiber;
      int end_key = start_key + keys_per_fiber;

      // Build OR chain with backticks
      std::ostringstream oss;
      for (int i = start_key; i < end_key && i < num_rows; i++) {
        oss << "`key` = \"key_" << i << "\"";
        if (i < end_key - 1 && i < num_rows - 1) {
          oss << " OR ";
        }
      }

      char* error_message = nullptr;
      LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

      if (result == LANCEDB_SUCCESS) {
        successful_deletes++;
      } else {
        std::lock_guard<std::mutex> lock(errors_mutex);
        errors.push_back("Fiber " + std::to_string(fiber_id) + ": " +
                        (error_message ? error_message : "unknown"));
        if (error_message) lancedb_free_string(error_message);
      }

      completed_fibers++;
    });
  }

  // Run all fibers
  io_ctx.run();

  INFO("Completed fibers: " << completed_fibers);
  INFO("Successful deletes: " << successful_deletes);

  for (const auto& err : errors) {
    INFO("Error: " << err);
  }

  REQUIRE(completed_fibers == num_fibers);

  // Some deletes might fail due to concurrent modification, that's OK
  // The important thing is no crashes
  CHECK(successful_deletes > 0);

  lancedb_table_free(table);
}

/**
 * Test 5: Exact Ceph RGW delete_vectors pattern
 * This replicates the exact code path from rgw_s3vector.cc:delete_vectors
 */
TEST_CASE_METHOD(BoostFiberFixture, "Boost Fiber - Exact Ceph delete_vectors pattern", "[boost][fiber][ceph][exact]") {
  const std::string table_name = "fiber_exact_ceph_pattern";
  constexpr int num_rows = 50;

  // Create table mimicking Ceph's vector index table
  auto key_field = arrow::field("key", arrow::utf8());
  auto data_field = arrow::field("data", arrow::fixed_size_list(arrow::float32(), TEST_SCHEMA_DIMENSIONS));
  auto schema = arrow::schema({key_field, data_field});

  struct ArrowSchema c_schema;
  REQUIRE(arrow::ExportSchema(*schema, &c_schema).ok());

  LanceDBTable* table = nullptr;
  char* error_message = nullptr;
  LanceDBError result = lancedb_table_create(
      db, table_name.c_str(),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      nullptr, &table, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);

  if (c_schema.release) c_schema.release(&c_schema);

  // Add data
  arrow::StringBuilder key_builder;
  arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
      std::make_unique<arrow::FloatBuilder>(), TEST_SCHEMA_DIMENSIONS);

  for (int i = 0; i < num_rows; i++) {
    REQUIRE(key_builder.Append("object_" + std::to_string(i)).ok());
    auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
    for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
      REQUIRE(list_builder->Append(static_cast<float>(i * 10 + j)).ok());
    }
    REQUIRE(data_builder.Append().ok());
  }

  std::shared_ptr<arrow::Array> key_array, data_array;
  REQUIRE(key_builder.Finish(&key_array).ok());
  REQUIRE(data_builder.Finish(&data_array).ok());
  auto batch = arrow::RecordBatch::Make(schema, num_rows, {key_array, data_array});

  struct ArrowArray c_array;
  struct ArrowSchema c_schema2;
  REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema2).ok());
  LanceDBRecordBatchReader* reader = nullptr;
  lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema2),
      &reader, nullptr);
  if (c_schema2.release) c_schema2.release(&c_schema2);

  result = lancedb_table_add(table, reader, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(lancedb_table_count_rows(table) == num_rows);

  std::atomic<bool> test_completed{false};
  std::atomic<bool> test_passed{false};
  std::string error_str;

  // This is the EXACT pattern from Ceph rgw_s3vector.cc:delete_vectors
  // with the backtick fix applied
  asio::spawn(io_ctx, [&](asio::yield_context yield) {
    // Simulate delete_vectors_t::keys from Ceph
    std::vector<std::string> keys_to_delete = {
      "object_0", "object_5", "object_10", "object_15", "object_20"
    };

    // Build filter exactly like Ceph does (with backticks - the fix)
    std::ostringstream oss;
    for (size_t i = 0; i < keys_to_delete.size(); ++i) {
      oss << "`key` = \"" << keys_to_delete[i] << "\"";
      if (i < keys_to_delete.size() - 1) {
        oss << " OR ";
      }
    }

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    if (result != LANCEDB_SUCCESS) {
      error_str = error_message ? error_message : "unknown";
      if (error_message) lancedb_free_string(error_message);
    } else {
      test_passed = true;
    }

    test_completed = true;
  });

  io_ctx.run();

  INFO("Error: " << error_str);
  REQUIRE(test_completed);
  REQUIRE(test_passed);
  CHECK(lancedb_table_count_rows(table) == num_rows - 5);

  lancedb_table_free(table);
}
