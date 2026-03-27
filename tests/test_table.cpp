/*
 * SPDX-License-Identifier: Apache-2.0
 * SPDX-FileCopyrightText: Copyright The LanceDB Authors
 */

#include <string>
#include "test_common.h"

TEST_CASE_METHOD(LanceDBFixture, "LanceDB Table Creation", "[table]") {
  SECTION("Create empty table") {
    create_empty_table("empty_table");
  }

  SECTION("Create table with data") {
    constexpr auto row_num = 10;
    LanceDBTable* table = create_table_with_data("table_with_data", row_num, 0);
    REQUIRE(lancedb_table_count_rows(table) == row_num);
    lancedb_table_free(table);
  }

  SECTION("Create table with data then reopen and verify") {
    const std::string table_name = "table_reopen_test";
    constexpr auto row_num = 15;
    LanceDBTable* table = create_table_with_data(table_name, row_num, 0);
    REQUIRE(lancedb_table_count_rows(table) == row_num);
    lancedb_table_free(table);

    // Reopen the table
    LanceDBTable* reopened_table = lancedb_connection_open_table(db, table_name.c_str());
    REQUIRE(reopened_table != nullptr);
    REQUIRE(lancedb_table_count_rows(reopened_table) == row_num);
    lancedb_table_free(reopened_table);
  }

  SECTION("Create table that already exists should fail") {
    const std::string table_name = "duplicate_table";

    // First create the table
    LanceDBTable* table = create_table_with_data(table_name, 5, 0);
    lancedb_table_free(table);

    // Try to create the same table again
    auto schema = create_test_schema();
    auto batch = create_test_record_batch(10, 0);
    auto reader = create_reader_from_batch(batch);
    REQUIRE(reader != nullptr);

    struct ArrowSchema c_schema;
    REQUIRE(arrow::ExportSchema(*schema, &c_schema).ok());

    LanceDBTable* table2 = nullptr;
    char* error_message = nullptr;

    LanceDBError result = lancedb_table_create(
        db,
        table_name.c_str(),
        reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
        reader,
        &table2,
        &error_message
    );

    REQUIRE(result == LANCEDB_TABLE_ALREADY_EXISTS);

    if (error_message) {
      lancedb_free_string(error_message);
    }

    // Note: Reader was consumed by lancedb_table_create even on failure

    // Clean up schema
    if (c_schema.release) {
      c_schema.release(&c_schema);
    }
  }
}

TEST_CASE_METHOD(LanceDBFixture, "LanceDB Table Add", "[table]") {
  // Create a test table
  const std::string table_name = "test_add_table";
  create_empty_table(table_name);

  // Open the table
  LanceDBTable* table = lancedb_connection_open_table(db, table_name.c_str());
  REQUIRE(table != nullptr);

  SECTION("Add data to empty table") {
    // Verify table is initially empty
    REQUIRE(lancedb_table_count_rows(table) == 0);

    // Initial version should be 1 (empty table)
    auto version = lancedb_table_version(table);
    REQUIRE(version == 1);

    constexpr auto row_num = 10;

    // Create and add a batch of data
    auto batch = create_test_record_batch(row_num, 0);
    auto reader = create_reader_from_batch(batch);
    REQUIRE(reader != nullptr);

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_add(table, reader, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // Verify row count
    REQUIRE(lancedb_table_count_rows(table) == row_num);

    // Version should increment to 2
    version = lancedb_table_version(table);
    REQUIRE(version == 2);
  }

  SECTION("Add multiple batches of data") {
    // Initial version should be 1 (empty table)
    auto version = lancedb_table_version(table);
    REQUIRE(version == 1);

    // Add first batch
    constexpr auto row_num1 = 5;
    auto batch1 = create_test_record_batch(row_num1, 0);
    auto reader1 = create_reader_from_batch(batch1);
    REQUIRE(reader1 != nullptr);

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_add(table, reader1, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num1);

    // Version should increment to 2
    version = lancedb_table_version(table);
    REQUIRE(version == 2);

    // Add second batch
    constexpr auto row_num2 = 7;
    auto batch2 = create_test_record_batch(row_num2, row_num1);
    auto reader2 = create_reader_from_batch(batch2);
    REQUIRE(reader2 != nullptr);

    result = lancedb_table_add(table, reader2, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num1+row_num2);

    // Version should increment to 3
    version = lancedb_table_version(table);
    REQUIRE(version == 3);
  }

  SECTION("Add data with duplicate keys creates duplicate rows") {
    // Add initial data with keys 0-9
    constexpr auto row_num = 10;
    auto batch1 = create_test_record_batch(row_num, 0);
    auto reader1 = create_reader_from_batch(batch1);
    REQUIRE(reader1 != nullptr);

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_add(table, reader1, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num);

    // Add data with overlapping keys (5-14)
    // Keys 5-9 already exist in the table
    constexpr auto overlap_start = 5;
    constexpr auto overlap_count = 10;
    auto batch2 = create_test_record_batch(overlap_count, overlap_start);
    auto reader2 = create_reader_from_batch(batch2);
    REQUIRE(reader2 != nullptr);

    result = lancedb_table_add(table, reader2, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // table_add adds all rows
    // So we should have 10 (original) + 10 (new batch) = 20 rows
    // Even though keys 5-9 exist in both batches
    REQUIRE(lancedb_table_count_rows(table) == 20);

    // Version should increment
    auto version = lancedb_table_version(table);
    REQUIRE(version == 3);
  }

  SECTION("Add data with null reader should fail") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_add(table, nullptr, &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);

    if (error_message) {
      lancedb_free_string(error_message);
    }
  }

  SECTION("Add data to null table should fail") {
    auto batch = create_test_record_batch(5, 0);
    auto reader = create_reader_from_batch(batch);
    REQUIRE(reader != nullptr);

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_add(nullptr, reader, &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);

    // Reader was not consumed due to error, must free it
    lancedb_record_batch_reader_free(reader);

    if (error_message) {
      lancedb_free_string(error_message);
    }
  }

  lancedb_table_free(table);
}

TEST_CASE_METHOD(LanceDBFixture, "LanceDB Table Merge Insert", "[table]") {
  // Create a test table with initial data
  const std::string table_name = "test_merge_table";
  create_empty_table(table_name);

  // Open the table
  LanceDBTable* table = lancedb_connection_open_table(db, table_name.c_str());
  REQUIRE(table != nullptr);

  // Add initial data
  constexpr auto row_num = 10;
  auto initial_batch = create_test_record_batch(row_num, 0);
  auto initial_reader = create_reader_from_batch(initial_batch);
  REQUIRE(initial_reader != nullptr);

  char* error_message = nullptr;
  LanceDBError result = lancedb_table_add(table, initial_reader, &error_message);

  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(error_message == nullptr);
  REQUIRE(lancedb_table_count_rows(table) == row_num);

  // Initial version after add should be 2 (1 for empty table creation, 2 after add)
  auto version = lancedb_table_version(table);
  REQUIRE(version == 2);

  SECTION("Merge insert with update and insert") {
    // Create data with some overlapping keys (0-4) and some new keys (10-14)
    auto schema = create_test_schema();

    arrow::StringBuilder key_builder;
    arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
        std::make_unique<arrow::FloatBuilder>(), TEST_SCHEMA_DIMENSIONS);

    // Add overlapping keys (should update)
    for (int i = 0; i < 5; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
        REQUIRE(list_builder->Append(static_cast<float>(999 + i)).ok());  // Different values
      }
      REQUIRE(data_builder.Append().ok());
    }

    // Add new keys (should insert)
    for (int i = 10; i < 15; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
        REQUIRE(list_builder->Append(static_cast<float>(i * 10 + j)).ok());
      }
      REQUIRE(data_builder.Append().ok());
    }

    std::shared_ptr<arrow::Array> key_array, data_array;
    REQUIRE(key_builder.Finish(&key_array).ok());
    REQUIRE(data_builder.Finish(&data_array).ok());

    auto merge_batch = arrow::RecordBatch::Make(schema, 10, {key_array, data_array});
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    const char* on_columns[] = {"key"};
    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 1
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, merge_reader, on_columns, 1, &config, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // Should have 10 (original) - 5 (overlapping) + 10 (total in merge) = 15 rows
    REQUIRE(lancedb_table_count_rows(table) == 15);

    // Version should increment to 3 (was 2 before merge insert)
    auto version = lancedb_table_version(table);
    REQUIRE(version == 3);
  }

  SECTION("Merge insert with update only") {
    // Create data with only overlapping keys
    auto schema = create_test_schema();

    arrow::StringBuilder key_builder;
    arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
        std::make_unique<arrow::FloatBuilder>(), TEST_SCHEMA_DIMENSIONS);

    for (int i = 0; i < 5; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
        REQUIRE(list_builder->Append(static_cast<float>(888 + i)).ok());
      }
      REQUIRE(data_builder.Append().ok());
    }

    std::shared_ptr<arrow::Array> key_array, data_array;
    REQUIRE(key_builder.Finish(&key_array).ok());
    REQUIRE(data_builder.Finish(&data_array).ok());

    auto merge_batch = arrow::RecordBatch::Make(schema, 5, {key_array, data_array});
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    const char* on_columns[] = {"key"};
    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 0  // Don't insert new rows
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, merge_reader, on_columns, 1, &config, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // Should still have 10 rows (only updates, no inserts)
    REQUIRE(lancedb_table_count_rows(table) == 10);

    // Version should increment to 3 (was 2 before merge insert)
    auto version = lancedb_table_version(table);
    REQUIRE(version == 3);
  }

  SECTION("Merge insert with insert only") {
    // Create data with only new keys
    auto schema = create_test_schema();

    arrow::StringBuilder key_builder;
    arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
        std::make_unique<arrow::FloatBuilder>(), TEST_SCHEMA_DIMENSIONS);

    for (int i = 20; i < 25; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
        REQUIRE(list_builder->Append(static_cast<float>(i * 10 + j)).ok());
      }
      REQUIRE(data_builder.Append().ok());
    }

    std::shared_ptr<arrow::Array> key_array, data_array;
    REQUIRE(key_builder.Finish(&key_array).ok());
    REQUIRE(data_builder.Finish(&data_array).ok());

    auto merge_batch = arrow::RecordBatch::Make(schema, 5, {key_array, data_array});
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    const char* on_columns[] = {"key"};
    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 0,  // Don't update existing rows
      .when_not_matched_insert_all = 1
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, merge_reader, on_columns, 1, &config, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // Should have 10 + 5 = 15 rows (only inserts, no updates)
    REQUIRE(lancedb_table_count_rows(table) == 15);

    // Version should increment to 3 (was 2 before merge insert)
    auto version = lancedb_table_version(table);
    REQUIRE(version == 3);
  }

  SECTION("Merge insert with null config uses defaults") {
    auto merge_batch = create_test_record_batch(3, 0);
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    const char* on_columns[] = {"key"};

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, merge_reader, on_columns, 1, nullptr, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // Default behavior should handle the merge
    REQUIRE(lancedb_table_count_rows(table) >= 10);

    // Version should increment to 3 (was 2 before merge insert)
    auto version = lancedb_table_version(table);
    REQUIRE(version == 3);
  }

  SECTION("Merge insert with no actual changes") {
    // Get current version
    auto version = lancedb_table_version(table);
    REQUIRE(version == 2);

    // Create data with same keys and same values as existing data
    auto schema = create_test_schema();

    arrow::StringBuilder key_builder;
    arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
        std::make_unique<arrow::FloatBuilder>(), TEST_SCHEMA_DIMENSIONS);

    // Use exact same data as initial batch (keys 0-4)
    for (int i = 0; i < 5; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
      for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
        REQUIRE(list_builder->Append(static_cast<float>(i * 10 + j)).ok());
      }
      REQUIRE(data_builder.Append().ok());
    }

    std::shared_ptr<arrow::Array> key_array, data_array;
    REQUIRE(key_builder.Finish(&key_array).ok());
    REQUIRE(data_builder.Finish(&data_array).ok());

    auto merge_batch = arrow::RecordBatch::Make(schema, 5, {key_array, data_array});
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    const char* on_columns[] = {"key"};
    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 0
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, merge_reader, on_columns, 1, &config, &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);

    // Row count should remain 10 (no new rows)
    REQUIRE(lancedb_table_count_rows(table) == 10);

    // Check if version changed even though data is identical
    version = lancedb_table_version(table);
    // Version increments even if data doesn't actually change
    REQUIRE(version == 3);
  }

  SECTION("Merge insert with null reader should fail") {
    const char* on_columns[] = {"key"};
    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 1
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, nullptr, on_columns, 1, &config, &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);

    if (error_message) {
      lancedb_free_string(error_message);
    }
  }

  SECTION("Merge insert with null table should fail") {
    auto merge_batch = create_test_record_batch(3, 0);
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    const char* on_columns[] = {"key"};
    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 1
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        nullptr, merge_reader, on_columns, 1, &config, &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);

    // Reader was not consumed due to error, must free it
    lancedb_record_batch_reader_free(merge_reader);

    if (error_message) {
      lancedb_free_string(error_message);
    }
  }

  SECTION("Merge insert with null on_columns should fail") {
    auto merge_batch = create_test_record_batch(3, 0);
    auto merge_reader = create_reader_from_batch(merge_batch);
    REQUIRE(merge_reader != nullptr);

    LanceDBMergeInsertConfig config = {
      .when_matched_update_all = 1,
      .when_not_matched_insert_all = 1
    };

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_merge_insert(
        table, merge_reader, nullptr, 1, &config, &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);

    // Reader was not consumed due to error, must free it
    lancedb_record_batch_reader_free(merge_reader);

    if (error_message) {
      lancedb_free_string(error_message);
    }
  }

  lancedb_table_free(table);
}

TEST_CASE_METHOD(LanceDBFixture, "LanceDB Table Delete", "[table]") {
  const std::string table_name = "test_delete_table";
  create_empty_table(table_name);

  LanceDBTable* table = lancedb_connection_open_table(db, table_name.c_str());
  REQUIRE(table != nullptr);

  // Add initial data (keys key_0 through key_9)
  constexpr auto row_num = 10;
  auto initial_batch = create_test_record_batch(row_num, 0);
  auto initial_reader = create_reader_from_batch(initial_batch);
  REQUIRE(initial_reader != nullptr);

  char* error_message = nullptr;
  LanceDBError result = lancedb_table_add(table, initial_reader, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(error_message == nullptr);
  REQUIRE(lancedb_table_count_rows(table) == row_num);

  SECTION("Delete row matching predicate") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, "key = 'key_0'", &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num - 1);
  }

  SECTION("Delete multiple rows matching predicate") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        table, "key IN ('key_0', 'key_1', 'key_2')", &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num - 3);
  }

  SECTION("Delete with predicate matching no rows") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        table, "key = 'nonexistent'", &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num);
  }

  SECTION("Delete all rows") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        table, "key IS NOT NULL", &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == 0);
  }

  SECTION("Delete with unknown column should fail") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        table, "unknown = 'key_0'", &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);
    REQUIRE(result != LANCEDB_SUCCESS);
    REQUIRE(error_message != nullptr);
    lancedb_free_string(error_message);
  }

  SECTION("Delete with empty predicate should fail") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        table, "", &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);
    REQUIRE(result != LANCEDB_SUCCESS);
    REQUIRE(error_message != nullptr);
    lancedb_free_string(error_message);
  }

  SECTION("Delete with null table should fail") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        nullptr, "key = 'key_0'", &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);
    REQUIRE(error_message != nullptr);
    lancedb_free_string(error_message);
  }

  SECTION("Delete with null predicate should fail") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(
        table, nullptr, &error_message);

    REQUIRE(result != LANCEDB_SUCCESS);
    REQUIRE(error_message != nullptr);
    lancedb_free_string(error_message);
  }

  lancedb_table_free(table);
}

// Test case mimicking Ceph RGW delete_vectors code path
// This tests the OR-based filter pattern used in rgw_s3vector.cc:delete_vectors
// The crash occurred when using 'key' (reserved SQL keyword) without backticks
TEST_CASE_METHOD(LanceDBFixture, "LanceDB Delete - Ceph RGW style OR filter", "[table][ceph]") {
  const std::string table_name = "delete_ceph_style_test";

  // Create table with data
  LanceDBTable* table = create_table_with_data(table_name, 0, 0);
  REQUIRE(table != nullptr);

  // Add initial data (keys key_0 through key_99)
  constexpr auto row_num = 100;
  auto initial_batch = create_test_record_batch(row_num, 0);
  auto initial_reader = create_reader_from_batch(initial_batch);
  REQUIRE(initial_reader != nullptr);

  char* error_message = nullptr;
  LanceDBError result = lancedb_table_add(table, initial_reader, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(error_message == nullptr);
  REQUIRE(lancedb_table_count_rows(table) == row_num);

  SECTION("Delete with OR filter using backticks (Ceph style - should work)") {
    // This is the pattern used in Ceph rgw_s3vector.cc:delete_vectors
    // Uses backticks around 'key' because it's a reserved SQL keyword
    std::ostringstream oss;
    std::vector<std::string> keys_to_delete = {"key_10", "key_20", "key_30", "key_40", "key_50"};
    for (size_t i = 0; i < keys_to_delete.size(); ++i) {
      oss << "`key` = \"" << keys_to_delete[i] << "\"";
      if (i < keys_to_delete.size() - 1) {
        oss << " OR ";
      }
    }

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num - 5);
  }

  SECTION("Delete with OR filter WITHOUT backticks (potential crash case)") {
    // This is the pattern that caused issues in Ceph before the fix
    // 'key' is a reserved SQL keyword and may cause parsing issues
    std::ostringstream oss;
    std::vector<std::string> keys_to_delete = {"key_10", "key_20", "key_30", "key_40", "key_50"};
    for (size_t i = 0; i < keys_to_delete.size(); ++i) {
      oss << "key = \"" << keys_to_delete[i] << "\"";
      if (i < keys_to_delete.size() - 1) {
        oss << " OR ";
      }
    }

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    // Note: This may or may not succeed depending on the SQL parser behavior
    // The test documents the behavior - if it fails, backticks are required
    if (result == LANCEDB_SUCCESS) {
      REQUIRE(error_message == nullptr);
      REQUIRE(lancedb_table_count_rows(table) == row_num - 5);
    } else {
      // If it fails, document that backticks are needed for reserved keywords
      WARN("Delete without backticks failed - use backticks for reserved keywords like 'key'");
      REQUIRE(error_message != nullptr);
      lancedb_free_string(error_message);
    }
  }

  SECTION("Delete single key with backticks") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, "`key` = \"key_0\"", &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num - 1);
  }

  SECTION("Delete single key without backticks") {
    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, "key = \"key_0\"", &error_message);

    // Document behavior - may or may not work
    if (result == LANCEDB_SUCCESS) {
      REQUIRE(error_message == nullptr);
      REQUIRE(lancedb_table_count_rows(table) == row_num - 1);
    } else {
      WARN("Delete without backticks failed for single key");
      REQUIRE(error_message != nullptr);
      lancedb_free_string(error_message);
    }
  }

  SECTION("Delete with many OR clauses (stress test like Ceph batch delete)") {
    // Ceph allows up to 500 keys in delete_vectors
    std::ostringstream oss;
    constexpr size_t num_keys = 50;  // Use 50 for reasonable test time
    for (size_t i = 0; i < num_keys; ++i) {
      oss << "`key` = \"key_" << i << "\"";
      if (i < num_keys - 1) {
        oss << " OR ";
      }
    }

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(error_message == nullptr);
    REQUIRE(lancedb_table_count_rows(table) == row_num - num_keys);
  }

  lancedb_table_free(table);
}

// Test with special characters in key values that could break SQL parsing
// This mimics real S3 keys which can contain almost any character
TEST_CASE_METHOD(LanceDBFixture, "LanceDB Delete - Special characters in keys", "[table][ceph][special]") {
  const std::string table_name = "delete_special_chars_test";

  // Create schema with key and data columns
  auto key_field = arrow::field("key", arrow::utf8());
  auto data_field = arrow::field("data", arrow::fixed_size_list(arrow::float32(), TEST_SCHEMA_DIMENSIONS));
  auto schema = arrow::schema({key_field, data_field});

  // Convert to C ABI
  struct ArrowSchema c_schema;
  REQUIRE(arrow::ExportSchema(*schema, &c_schema).ok());

  // Create table
  LanceDBTable* table = nullptr;
  char* error_message = nullptr;
  LanceDBError result = lancedb_table_create(
      db, table_name.c_str(),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      nullptr, &table, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(table != nullptr);

  if (c_schema.release) {
    c_schema.release(&c_schema);
  }

  // Create test data with special characters in keys
  // These are characters that could potentially break SQL parsing
  std::vector<std::string> special_keys = {
    "normal_key",
    "key with spaces",
    "key/with/slashes",
    "key\"with\"quotes",       // Double quotes - could break SQL string
    "key\\with\\backslashes",  // Backslashes
    "key'with'single'quotes",  // Single quotes
    "key OR other",            // SQL keyword in value
    "key AND value",           // SQL keyword in value
    "key = value",             // SQL operator in value
    "key`with`backticks",      // Backticks - could break column quoting
  };

  // Build record batch with special keys
  arrow::StringBuilder key_builder;
  arrow::FixedSizeListBuilder data_builder(arrow::default_memory_pool(),
      std::make_unique<arrow::FloatBuilder>(), TEST_SCHEMA_DIMENSIONS);

  for (size_t i = 0; i < special_keys.size(); i++) {
    REQUIRE(key_builder.Append(special_keys[i]).ok());
    auto list_builder = static_cast<arrow::FloatBuilder*>(data_builder.value_builder());
    for (size_t j = 0; j < TEST_SCHEMA_DIMENSIONS; j++) {
      REQUIRE(list_builder->Append(static_cast<float>(i * 10 + j)).ok());
    }
    REQUIRE(data_builder.Append().ok());
  }

  std::shared_ptr<arrow::Array> key_array, data_array;
  REQUIRE(key_builder.Finish(&key_array).ok());
  REQUIRE(data_builder.Finish(&data_array).ok());

  auto batch = arrow::RecordBatch::Make(schema, special_keys.size(), {key_array, data_array});

  // Add data to table
  struct ArrowArray c_array;
  struct ArrowSchema c_schema2;
  REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema2).ok());

  LanceDBRecordBatchReader* reader = nullptr;
  lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema2),
      &reader, nullptr);

  if (c_schema2.release) {
    c_schema2.release(&c_schema2);
  }

  result = lancedb_table_add(table, reader, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(lancedb_table_count_rows(table) == special_keys.size());

  SECTION("Delete key with double quotes - POTENTIAL CRASH") {
    // This could cause SQL injection-like issues
    // Filter: `key` = "key"with"quotes"  <- broken SQL!
    std::ostringstream oss;
    oss << "`key` = \"" << "key\"with\"quotes" << "\"";

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    // This will likely fail due to unescaped quotes
    if (result != LANCEDB_SUCCESS) {
      INFO("Filter string: " << oss.str());
      INFO("Error: " << (error_message ? error_message : "null"));
      WARN("Delete with embedded quotes failed - keys with quotes need escaping");
      if (error_message) lancedb_free_string(error_message);
    }
  }

  SECTION("Delete key with SQL keywords") {
    // Filter: `key` = "key OR other"
    // The OR is inside quotes so should be safe
    std::ostringstream oss;
    oss << "`key` = \"" << "key OR other" << "\"";

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    if (result == LANCEDB_SUCCESS) {
      REQUIRE(lancedb_table_count_rows(table) == special_keys.size() - 1);
    } else {
      INFO("Filter string: " << oss.str());
      INFO("Error: " << (error_message ? error_message : "null"));
      WARN("Delete with SQL keyword in value failed");
      if (error_message) lancedb_free_string(error_message);
    }
  }

  SECTION("Delete key with backticks in value") {
    // Filter: `key` = "key`with`backticks"
    // Backticks in value could confuse the parser
    std::ostringstream oss;
    oss << "`key` = \"" << "key`with`backticks" << "\"";

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    if (result != LANCEDB_SUCCESS) {
      INFO("Filter string: " << oss.str());
      INFO("Error: " << (error_message ? error_message : "null"));
      WARN("Delete with backticks in value failed");
      if (error_message) lancedb_free_string(error_message);
    }
  }

  SECTION("Delete multiple special keys with OR - Ceph pattern") {
    // This is the exact pattern from Ceph that could cause issues
    std::ostringstream oss;
    std::vector<std::string> keys_to_delete = {
      "key with spaces",
      "key/with/slashes",
      "key OR other"
    };

    for (size_t i = 0; i < keys_to_delete.size(); ++i) {
      oss << "`key` = \"" << keys_to_delete[i] << "\"";
      if (i < keys_to_delete.size() - 1) {
        oss << " OR ";
      }
    }

    char* error_message = nullptr;
    LanceDBError result = lancedb_table_delete(table, oss.str().c_str(), &error_message);

    INFO("Filter string: " << oss.str());
    if (result == LANCEDB_SUCCESS) {
      REQUIRE(error_message == nullptr);
      // Should have deleted 3 keys
      REQUIRE(lancedb_table_count_rows(table) == special_keys.size() - 3);
    } else {
      INFO("Error: " << (error_message ? error_message : "null"));
      WARN("Delete with special characters in OR pattern failed");
      if (error_message) lancedb_free_string(error_message);
    }
  }

  lancedb_table_free(table);
}

// Test that triggers Column::from_qualified_name code path in DataFusion/Lance
// This is the code path that caused the crash in Ceph RGW when using reserved
// SQL keywords as column names without proper quoting.
//
// Stack trace from crash:
//   lancedb_table_delete -> lance::dataset::delete -> Planner::parse_filter
//   -> Planner::column -> datafusion_expr::col() -> Column::from_qualified_name
//   -> parse_identifiers_normalized -> Parser::try_with_sql
//   -> Tokenizer::next_token (self=0x0) <- NULL pointer crash!
//
// The issue: Column::from_qualified_name re-tokenizes the column name,
// and reserved SQL keywords cause the tokenizer to fail.
TEST_CASE_METHOD(LanceDBFixture, "LanceDB Delete - Column::from_qualified_name reserved keyword", "[table][ceph][datafusion]") {

  SECTION("Test with 'key' column - reserved SQL keyword") {
    const std::string table_name = "reserved_keyword_key_test";

    // Create schema with 'key' column (KEY is a reserved SQL keyword)
    auto key_field = arrow::field("key", arrow::utf8());
    auto value_field = arrow::field("value", arrow::int32());
    auto schema = arrow::schema({key_field, value_field});

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

    // Add test data
    arrow::StringBuilder key_builder;
    arrow::Int32Builder value_builder;
    for (int i = 0; i < 10; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      REQUIRE(value_builder.Append(i * 100).ok());
    }
    std::shared_ptr<arrow::Array> key_array, value_array;
    REQUIRE(key_builder.Finish(&key_array).ok());
    REQUIRE(value_builder.Finish(&value_array).ok());
    auto batch = arrow::RecordBatch::Make(schema, 10, {key_array, value_array});

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
    REQUIRE(lancedb_table_count_rows(table) == 10);

    // TEST 1: Delete with backticks (should always work)
    // This triggers: Planner::column -> Column::from_qualified_name
    // But backticks make it a quoted identifier, bypassing keyword issues
    result = lancedb_table_delete(table, "`key` = \"key_0\"", &error_message);
    INFO("Delete with backticks - error: " << (error_message ? error_message : "none"));
    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(lancedb_table_count_rows(table) == 9);

    // TEST 2: Delete WITHOUT backticks - this is what caused the Ceph crash
    // key = "key_1" triggers Column::from_qualified_name("key")
    // which calls parse_identifiers("key") - KEY is reserved!
    result = lancedb_table_delete(table, "key = \"key_1\"", &error_message);
    if (result == LANCEDB_SUCCESS) {
      INFO("Delete without backticks succeeded (parser handles reserved keyword)");
      REQUIRE(lancedb_table_count_rows(table) == 8);
    } else {
      // This is the crash/error case - document the behavior
      INFO("Delete without backticks failed!");
      INFO("Error: " << (error_message ? error_message : "null/crash"));
      WARN("RESERVED KEYWORD BUG: 'key' without backticks fails in Column::from_qualified_name");
      if (error_message) lancedb_free_string(error_message);
    }

    // TEST 3: Multiple OR clauses without backticks (exact Ceph pattern)
    // This creates deep recursion in Planner::binary_expr
    std::string filter = "key = \"key_2\" OR key = \"key_3\" OR key = \"key_4\"";
    result = lancedb_table_delete(table, filter.c_str(), &error_message);
    if (result == LANCEDB_SUCCESS) {
      INFO("OR chain without backticks succeeded");
    } else {
      INFO("OR chain without backticks failed!");
      INFO("Filter: " << filter);
      INFO("Error: " << (error_message ? error_message : "null/crash"));
      WARN("RESERVED KEYWORD BUG: OR chain with 'key' fails");
      if (error_message) lancedb_free_string(error_message);
    }

    lancedb_table_free(table);
  }

  SECTION("Test with other reserved SQL keywords as column names") {
    // Test columns named with various SQL reserved keywords
    // These should all require quoting to work reliably
    std::vector<std::string> reserved_keywords = {
      "select", "from", "where", "and", "or", "not", "in",
      "order", "by", "group", "having", "join", "on",
      "insert", "update", "delete", "create", "drop", "table",
      "index", "primary", "foreign", "key", "unique", "null",
      "true", "false", "as", "is", "like", "between"
    };

    for (const auto& keyword : reserved_keywords) {
      const std::string table_name = "reserved_" + keyword + "_test";

      // Create table with reserved keyword as column name
      auto col_field = arrow::field(keyword, arrow::utf8());
      auto id_field = arrow::field("id", arrow::int32());
      auto schema = arrow::schema({col_field, id_field});

      struct ArrowSchema c_schema;
      if (!arrow::ExportSchema(*schema, &c_schema).ok()) continue;

      LanceDBTable* table = nullptr;
      char* error_message = nullptr;
      LanceDBError result = lancedb_table_create(
          db, table_name.c_str(),
          reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
          nullptr, &table, &error_message);

      if (c_schema.release) c_schema.release(&c_schema);

      if (result != LANCEDB_SUCCESS) {
        if (error_message) lancedb_free_string(error_message);
        continue;
      }

      // Add one row
      arrow::StringBuilder col_builder;
      arrow::Int32Builder id_builder;
      REQUIRE(col_builder.Append("test_value").ok());
      REQUIRE(id_builder.Append(1).ok());
      std::shared_ptr<arrow::Array> col_array, id_array;
      REQUIRE(col_builder.Finish(&col_array).ok());
      REQUIRE(id_builder.Finish(&id_array).ok());
      auto batch = arrow::RecordBatch::Make(schema, 1, {col_array, id_array});

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
      if (result != LANCEDB_SUCCESS) {
        if (error_message) lancedb_free_string(error_message);
        lancedb_table_free(table);
        continue;
      }

      // Test delete with backticks (should work)
      std::string filter_quoted = "`" + keyword + "` = \"test_value\"";
      result = lancedb_table_delete(table, filter_quoted.c_str(), &error_message);
      if (result != LANCEDB_SUCCESS) {
        INFO("Keyword '" << keyword << "' with backticks failed: "
             << (error_message ? error_message : "unknown"));
        if (error_message) lancedb_free_string(error_message);
      }

      // Re-add data for unquoted test
      REQUIRE(col_builder.Append("test_value2").ok());
      REQUIRE(id_builder.Append(2).ok());
      REQUIRE(col_builder.Finish(&col_array).ok());
      REQUIRE(id_builder.Finish(&id_array).ok());
      batch = arrow::RecordBatch::Make(schema, 1, {col_array, id_array});
      REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema2).ok());
      lancedb_record_batch_reader_from_arrow(
          reinterpret_cast<FFI_ArrowArray*>(&c_array),
          reinterpret_cast<FFI_ArrowSchema*>(&c_schema2),
          &reader, nullptr);
      if (c_schema2.release) c_schema2.release(&c_schema2);
      lancedb_table_add(table, reader, &error_message);

      // Test delete WITHOUT backticks - may crash or fail
      std::string filter_unquoted = keyword + " = \"test_value2\"";
      result = lancedb_table_delete(table, filter_unquoted.c_str(), &error_message);
      if (result != LANCEDB_SUCCESS) {
        INFO("Keyword '" << keyword << "' WITHOUT backticks failed (expected for reserved keywords)");
        if (error_message) {
          INFO("Error: " << error_message);
          lancedb_free_string(error_message);
        }
      }

      lancedb_table_free(table);
    }
  }

  SECTION("Stress test - large OR chain (Ceph allows up to 500 keys)") {
    // Ceph allows up to 500 keys in delete_vectors
    // The deep recursion in binary_expr might cause stack overflow
    // especially on systems with limited stack (like boost::asio fibers)
    const std::string table_name = "stress_or_chain_test";

    auto key_field = arrow::field("key", arrow::utf8());
    auto value_field = arrow::field("value", arrow::int32());
    auto schema = arrow::schema({key_field, value_field});

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

    // Add 500 rows
    constexpr int num_rows = 500;
    arrow::StringBuilder key_builder;
    arrow::Int32Builder value_builder;
    for (int i = 0; i < num_rows; i++) {
      REQUIRE(key_builder.Append("key_" + std::to_string(i)).ok());
      REQUIRE(value_builder.Append(i).ok());
    }
    std::shared_ptr<arrow::Array> key_array, value_array;
    REQUIRE(key_builder.Finish(&key_array).ok());
    REQUIRE(value_builder.Finish(&value_array).ok());
    auto batch = arrow::RecordBatch::Make(schema, num_rows, {key_array, value_array});

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

    // Build large OR chain with 100 keys (without backticks - the crash case)
    std::ostringstream oss;
    constexpr int keys_to_delete = 100;
    for (int i = 0; i < keys_to_delete; i++) {
      oss << "key = \"key_" << i << "\"";
      if (i < keys_to_delete - 1) {
        oss << " OR ";
      }
    }

    INFO("Filter length: " << oss.str().length() << " chars");
    INFO("Number of OR clauses: " << keys_to_delete);

    result = lancedb_table_delete(table, oss.str().c_str(), &error_message);
    if (result == LANCEDB_SUCCESS) {
      INFO("Large OR chain without backticks succeeded");
      REQUIRE(lancedb_table_count_rows(table) == num_rows - keys_to_delete);
    } else {
      INFO("Large OR chain without backticks FAILED");
      INFO("Error: " << (error_message ? error_message : "crash/null"));
      WARN("POTENTIAL STACK OVERFLOW or reserved keyword issue with large OR chains");
      if (error_message) lancedb_free_string(error_message);
    }

    // Now test with backticks (should work)
    std::ostringstream oss2;
    for (int i = keys_to_delete; i < keys_to_delete * 2 && i < num_rows; i++) {
      oss2 << "`key` = \"key_" << i << "\"";
      if (i < keys_to_delete * 2 - 1 && i < num_rows - 1) {
        oss2 << " OR ";
      }
    }

    result = lancedb_table_delete(table, oss2.str().c_str(), &error_message);
    INFO("Large OR chain WITH backticks: " << (result == LANCEDB_SUCCESS ? "SUCCESS" : "FAILED"));
    if (result != LANCEDB_SUCCESS && error_message) {
      INFO("Error: " << error_message);
      lancedb_free_string(error_message);
    }

    lancedb_table_free(table);
  }
}

TEST_CASE_METHOD(LanceDBFixture, "LanceDB Create Reader", "[table]") {
  constexpr auto row_num = 10;
  auto batch = create_test_record_batch(row_num, 0);

  SECTION("Successfully create reader") {
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;

    REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema).ok());

    LanceDBRecordBatchReader* reader = nullptr;
    char* error_message = nullptr;
    // We expect success here
    LanceDBError result = lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      &reader,
      &error_message);

    REQUIRE(result == LANCEDB_SUCCESS);
    REQUIRE(reader);
    REQUIRE(error_message == nullptr);

    // According to docs/code: Schema is only read by the function, so we must release it.
    // The array IS CONSUMED by definition of the function (though technically checked early before consumption, if success occurs, array is consumed).
    if (c_schema.release) {
      c_schema.release(&c_schema);
    }

    // We must free the reader
    lancedb_record_batch_reader_free(reader);
  }

  SECTION("Create reader with null array") {
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;

    REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema).ok());

    LanceDBRecordBatchReader* reader = nullptr;
    char* error_message = nullptr;
    LanceDBError result = lancedb_record_batch_reader_from_arrow(
      nullptr,
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      &reader,
      &error_message);

    REQUIRE(result == LANCEDB_INVALID_ARGUMENT);
    REQUIRE(reader == nullptr);
    REQUIRE(error_message);

    lancedb_free_string(error_message);

    // Since it failed early (null check), array was not consumed.
    if (c_array.release) {
      c_array.release(&c_array);
    }
    if (c_schema.release) {
      c_schema.release(&c_schema);
    }
  }

  SECTION("Create reader with null schema") {
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;

    REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema).ok());

    LanceDBRecordBatchReader* reader = nullptr;
    char* error_message = nullptr;
    LanceDBError result = lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      nullptr,
      &reader,
      &error_message);

    REQUIRE(result == LANCEDB_INVALID_ARGUMENT);
    REQUIRE(reader == nullptr);
    REQUIRE(error_message);

    lancedb_free_string(error_message);

    // Since it failed early, array was not consumed.
    if (c_array.release) {
      c_array.release(&c_array);
    }
    if (c_schema.release) {
      c_schema.release(&c_schema);
    }
  }

  SECTION("Create reader with null output pointer") {
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;

    REQUIRE(arrow::ExportRecordBatch(*batch, &c_array, &c_schema).ok());

    char* error_message = nullptr;
    LanceDBError result = lancedb_record_batch_reader_from_arrow(
      reinterpret_cast<FFI_ArrowArray*>(&c_array),
      reinterpret_cast<FFI_ArrowSchema*>(&c_schema),
      nullptr,
      &error_message);

    REQUIRE(result == LANCEDB_INVALID_ARGUMENT);
    REQUIRE(error_message);

    lancedb_free_string(error_message);

    // Since it failed early, array was not consumed.
    if (c_array.release) {
      c_array.release(&c_array);
    }
    if (c_schema.release) {
      c_schema.release(&c_schema);
    }
  }
}
TEST_CASE_METHOD(LanceDBSessionFixture, "LanceDB Table CRUD with same session across multiple tables", "[table][session]") {
  const char* _namespace = nullptr;
  const std::string table_a_name = "session_table_a";
  const std::string table_b_name = "session_table_b";
  constexpr auto table_a_rows = 10;
  constexpr auto table_b_rows = 15;

  // Create
  LanceDBTable* table_a = create_table_with_data(table_a_name, table_a_rows, 0);
  LanceDBTable* table_b = create_table_with_data(table_b_name, table_b_rows, 0);
  REQUIRE(table_a != nullptr);
  REQUIRE(table_b != nullptr);

  // Read after create
  REQUIRE(lancedb_table_count_rows(table_a) == table_a_rows);
  REQUIRE(lancedb_table_count_rows(table_b) == table_b_rows);

  // Reopen and read again
  lancedb_table_free(table_a);
  lancedb_table_free(table_b);
  table_a = lancedb_connection_open_table(db, table_a_name.c_str());
  table_b = lancedb_connection_open_table(db, table_b_name.c_str());
  REQUIRE(table_a != nullptr);
  REQUIRE(table_b != nullptr);
  REQUIRE(lancedb_table_count_rows(table_a) == table_a_rows);
  REQUIRE(lancedb_table_count_rows(table_b) == table_b_rows);

  // Delete
  lancedb_table_free(table_a);
  lancedb_table_free(table_b);
  char* error_message = nullptr;
  LanceDBError result = lancedb_connection_drop_table(db, table_a_name.c_str(), _namespace, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(error_message == nullptr);

  result = lancedb_connection_drop_table(db, table_b_name.c_str(), _namespace, &error_message);
  REQUIRE(result == LANCEDB_SUCCESS);
  REQUIRE(error_message == nullptr);

  // Verify delete
  table_a = lancedb_connection_open_table(db, table_a_name.c_str());
  table_b = lancedb_connection_open_table(db, table_b_name.c_str());
  REQUIRE(table_a == nullptr);
  REQUIRE(table_b == nullptr);
}
