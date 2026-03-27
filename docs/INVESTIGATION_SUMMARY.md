# LanceDB-C Investigation Summary

This document summarizes the investigation and debugging session for LanceDB-C, focusing on a crash that occurred in Ceph RGW when using reserved SQL keywords.

---

## Table of Contents

1. [Running Individual Tests](#1-running-individual-tests)
2. [Disabling LanceDB/Lance Cache](#2-disabling-lancedbance-cache)
3. [Reserved SQL Keyword Issue (`key` column)](#3-reserved-sql-keyword-issue-key-column)
4. [Root Cause Analysis of Ceph Crash](#4-root-cause-analysis-of-ceph-crash)
5. [Test Cases Created](#5-test-cases-created)
6. [Boost::Asio Fiber Compatibility Issue](#6-boostasio-fiber-compatibility-issue)
7. [Recommendations](#7-recommendations)

---

## 1. Running Individual Tests

### Query
How to run individual test `tests/test_query.cpp` and check for crashes/failures.

### Solution
```bash
# Configure with tests enabled
cmake -B build -DBUILD_TESTS=ON

# Build specific test target
cmake --build build --target lancedb_query_tests

# Run the test directly
cd build
LD_LIBRARY_PATH=./target/release:$LD_LIBRARY_PATH ./lancedb_query_tests

# Or run via CTest with verbose output
ctest -R lancedb_query_tests -V

# Run specific test case using Catch2
./lancedb_query_tests "Test case name"
./lancedb_query_tests --list-tests  # List all tests
```

### Result
All 8368 assertions in 4 test cases passed.

---

## 2. Disabling LanceDB/Lance Cache

### Query
How to disable LanceDB/Lance cache.

### Solution

**Method 1: Via Session Options (C API)**
```cpp
LanceDBSessionOptions session_opts = {
    .index_cache_bytes = 1,      // Minimal cache (1 byte)
    .metadata_cache_bytes = 1    // Minimal cache (1 byte)
};
LanceDBSession* session = lancedb_session_new(&session_opts);
```

**Method 2: Environment Variables (No rebuild needed)**
```bash
export LANCE_FILE_METADATA_CACHE_SIZE=0
export LANCE_INDEX_CACHE_SIZE=0
./your_app
```

**Default Cache Sizes:**
- Index cache: 6 GB
- Metadata cache: 1 GB

---

## 3. Reserved SQL Keyword Issue (`key` column)

### Query
Why did using `key` (a reserved SQL keyword) as a column name crash in Ceph but work in lancedb-c tests?

### Background
- Ceph RGW code at `rgw_s3vector.cc:1268` builds SQL filters like:
  ```cpp
  oss << "key = \"" << configuration.keys[i] << "\"";
  ```
- This crashed when `key` was not quoted with backticks.
- Fix applied in commit `8c2126f83fc`:
  ```cpp
  oss << "`key` = \"" << configuration.keys[i] << "\"";
  ```

### Why Tests Passed But Ceph Crashed

The crash was **NOT** primarily about the reserved keyword. The stack trace revealed:

```
#0 sqlparser::tokenizer::Tokenizer::next_token (self=0x0, chars=0x0)
   <- NULL pointer dereference!
```

**Call Chain:**
```
lancedb_table_delete
  → lance::dataset::delete
    → Planner::parse_filter
      → Planner::column
        → Column::from_qualified_name
          → parse_identifiers_normalized
            → Parser::try_with_sql
              → Tokenizer::next_token (self=0x0) ← CRASH
```

The `Column::from_qualified_name` function re-tokenizes the column name, and this process corrupted memory when running inside boost::asio fibers.

---

## 4. Root Cause Analysis of Ceph Crash

### Stack Trace Location
`ceph/build/gdb.txt`

### Key Findings

1. **NULL Pointer Dereference**: `self=0x0` and `chars=0x0` in the tokenizer
2. **Deep Recursion**: 11+ levels of `Planner::binary_expr` calls for OR chains
3. **Fiber Environment**: Ceph uses `boost::asio::spawn` with fiber-based coroutines
4. **Memory Corruption**: Heap corruption leading to NULL pointers

### The Real Root Cause

**Tokio runtime incompatibility with boost::asio fibers:**

1. LanceDB uses `tokio::runtime::Runtime::block_on()` internally
2. Tokio expects a normal thread stack, not a fiber's limited stack (64KB-1MB)
3. Running tokio inside fibers causes stack/heap corruption
4. This corruption manifests as NULL pointers and `malloc()` errors

---

## 5. Test Cases Created

### 5.1 Ceph-Style Delete Tests (`test_table.cpp`)

**Tags:** `[ceph]`, `[datafusion]`, `[special]`

```cpp
// Test 1: OR filter with backticks (Ceph style)
"`key` = \"key_10\" OR `key` = \"key_20\""

// Test 2: OR filter WITHOUT backticks (crash case)
"key = \"key_10\" OR key = \"key_20\""

// Test 3: Special characters in key values
"key\"with\"quotes", "key OR other", "key`with`backticks"

// Test 4: Stress test with 100 keys
for (int i = 0; i < 100; i++) {
    oss << "`key` = \"key_" << i << "\"";
}
```

### 5.2 Boost Fiber Tests (`test_boost_fiber.cpp`)

**Tags:** `[boost]`, `[fiber]`, `[ceph]`

| Test | Description | Result |
|------|-------------|--------|
| Basic delete | Simple delete in fiber | PASSED |
| Large OR chain | 20 keys with OR | PASSED (with backticks) |
| Small stack stress | 50 keys, detached fiber | **CRASHED** |
| Concurrent deletes | Multiple fibers | PASSED |
| Exact Ceph pattern | Mimics rgw_s3vector.cc | PASSED |

### Running the Tests

```bash
cd build

# Run Ceph-style tests
./lancedb_table_tests "[ceph]"
./lancedb_table_tests "[datafusion]"

# Run Boost fiber tests
./lancedb_boost_fiber_tests "[fiber]"
```

---

## 6. Boost::Asio Fiber Compatibility Issue

### Problem

Running lancedb operations inside boost::asio fibers causes memory corruption:

```
malloc(): unaligned fastbin chunk detected
corrupted size vs. prev_size
SIGABRT - Abort (abnormal termination) signal
```

### Technical Details

1. **Tokio Runtime**: LanceDB Rust code uses tokio for async operations
2. **block_on()**: Synchronous calls use `runtime.block_on(async_operation)`
3. **Fiber Stack Limitation**: Fibers have limited stack (64KB-1MB vs 8MB for threads)
4. **Incompatibility**: Tokio's thread-local storage and stack usage conflicts with fibers

### Evidence

```
boost::context::detail::fiber_record<...>::run()
  → tokio::runtime::runtime::Runtime::block_on()
    → ... deep recursion ...
      → Memory corruption → NULL pointer
```

---

## 7. Recommendations

### For Ceph RGW

**Option 1: Post lancedb calls to a thread pool**
```cpp
// Don't call lancedb directly from fibers
asio::post(thread_pool, [&]() {
    lancedb_table_delete(table, filter.c_str(), &error);
});
// Wait for completion or use future
```

**Option 2: Use a dedicated thread for lancedb operations**
```cpp
std::thread lancedb_thread([&]() {
    lancedb_table_delete(table, filter.c_str(), &error);
});
lancedb_thread.join();
```

**Option 3: Keep using backticks (workaround)**
```cpp
// Always quote column names that might be reserved keywords
oss << "`key` = \"" << value << "\"";
```

### For LanceDB-C

1. Document the fiber incompatibility
2. Consider adding a warning when detecting fiber environment
3. Potentially provide a thread-safe wrapper that posts operations to a dedicated thread

### Best Practices for SQL Filters

```cpp
// Always use backticks for column names
"`column_name` = \"value\""

// Escape special characters in values
// (values are already quoted, but be careful with embedded quotes)

// For large OR chains, consider using IN clause if supported
"`key` IN (\"key_1\", \"key_2\", \"key_3\")"
```

---

## Files Modified/Created

| File | Description |
|------|-------------|
| `tests/test_table.cpp` | Added Ceph-style delete tests |
| `tests/test_boost_fiber.cpp` | New file - Boost fiber tests |
| `CMakeLists.txt` | Added `lancedb_boost_fiber_tests` target |

---

## Version Information

| Component | Version |
|-----------|---------|
| LanceDB | 0.22.3 |
| DataFusion | 50.3.0 |
| sqlparser | 0.58.0 |
| Boost | 1.83.0 |
| Rust | 1.91.1 |

---

## Summary

The investigation revealed that the Ceph RGW crash was caused by a **fundamental incompatibility between LanceDB's tokio runtime and boost::asio fibers**, not just the reserved SQL keyword issue. While the backticks fix works as a workaround, the proper solution is to avoid calling lancedb functions directly from within boost::asio fibers.
