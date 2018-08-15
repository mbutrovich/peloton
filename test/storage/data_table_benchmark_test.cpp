//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_benchmark_test.cpp
//
// Identification: test/storage/data_table_benchmark_test.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>
#include "common/harness.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/layout.h"
#include "storage/table_factory.h"
#include "storage/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

const int num_inserts = 10000000;
const uint32_t num_iterations = 3;
auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
auto column_a = catalog::Column(
    type::TypeId::BIGINT, type::Type::GetTypeSize(type::TypeId::BIGINT),
    "COL_A", true);
auto column_b = catalog::Column(
    type::TypeId::BIGINT, type::Type::GetTypeSize(type::TypeId::BIGINT),
    "COL_B", true);
catalog::Schema *schema = new catalog::Schema(
    {column_a, column_b});
ItemPointer *index_entry_ptr = nullptr;
storage::Tuple tuple(schema,true);

TEST(DataTableBenchmark, SimpleInsertWithCC_OneTransaction) {

  for (uint32_t i = 0; i < num_iterations; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
        INVALID_OID, INVALID_OID, schema, "test_table",
        1000, false, false));

    auto txn = txn_manager.BeginTransaction();
    for (int j = 0; j < num_inserts; j++) {
      ItemPointer tuple_slot_id = table->InsertTuple(&tuple, txn, &index_entry_ptr);
      txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
    }
    txn_manager.CommitTransaction(txn);
    delete txn;
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    std::cout << num_inserts / elapsed_seconds.count() / 1000000 << "M items/s" << std::endl;
  }
}

TEST(DataTableBenchmark, SimpleInsertWithCC_OneTransactionPerInsert) {

  for (uint32_t i = 0; i < num_iterations; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
        INVALID_OID, INVALID_OID, schema, "test_table",
        1000, false, false));

    for (int j = 0; j < num_inserts; j++) {
      auto txn = txn_manager.BeginTransaction();
      ItemPointer tuple_slot_id = table->InsertTuple(&tuple, txn, &index_entry_ptr);
      txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
      txn_manager.CommitTransaction(txn);
      delete txn;
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    std::cout << num_inserts / elapsed_seconds.count() / 1000000 << "M items/s" << std::endl;
  }
}

TEST(DataTableBenchmark, SimpleInsertWithoutCC) {

  for (uint32_t i = 0; i < num_iterations; i++) {
    auto start = std::chrono::high_resolution_clock::now();
    std::unique_ptr<storage::DataTable> table(storage::TableFactory::GetDataTable(
        INVALID_OID, INVALID_OID, schema, "test_table",
        1000, false, false));

    auto txn = new concurrency::TransactionContext(0, IsolationLevelType::SNAPSHOT, 0);
    for (int j = 0; j < num_inserts; j++) {
      table->InsertTuple(&tuple, txn, &index_entry_ptr);
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

    std::cout << num_inserts / elapsed_seconds.count() / 1000000 << "M items/s" << std::endl;
  }
}

}  // namespace test
}  // namespace peloton
