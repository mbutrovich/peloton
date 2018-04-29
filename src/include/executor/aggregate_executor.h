//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_executor.h
//
// Identification: src/include/executor/aggregate_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "executor/aggregator.h"

#include <vector>
#include <chrono>

namespace peloton {

namespace storage {
class DataTable;
}

namespace executor {

/**
 * The actual executor class templated on the type of aggregation that
 * should be performed.
 *
 * If it is instantiated using PlanNodeType::AGGREGATE,
 * then it will do a constant space aggregation that expects the input table
 * to be sorted on the group by key.
 *
 * If it is instantiated using PlanNodeType::HASHAGGREGATE,
 * then the input does not need to be sorted and it will hash the group by key
 * to aggregate the tuples.
 *
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class AggregateExecutor : public AbstractExecutor {
 public:
  AggregateExecutor(const AggregateExecutor &) = delete;
  AggregateExecutor &operator=(const AggregateExecutor &) = delete;
  AggregateExecutor(AggregateExecutor &&) = delete;
  AggregateExecutor &operator=(AggregateExecutor &&) = delete;

  AggregateExecutor(const planner::AbstractPlan *node,
                    ExecutorContext *executor_context);

  ~AggregateExecutor();


  static constexpr size_t num_threads_ = 8;
  static constexpr size_t num_phases_ = 5;
  std::chrono::duration<double> timers_[num_phases_][num_threads_];

 protected:
  bool DInit();

  bool DExecute();

  bool DExecuteSequential();
  bool DExecuteParallel();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of aggregate */
  std::vector<LogicalTile *> result;

  /** @brief Result itr */
  oid_t result_itr = INVALID_OID;

  /** @brief Computed the result */
  bool done = false;

  /** @brief Output table. */
  storage::AbstractTable *output_table = nullptr;

 private:
  std::thread threads_[num_threads_];
  std::shared_ptr<HashAggregateMapType> local_hash_tables_[num_threads_];
  std::shared_ptr<HashAggregateMapType> global_hash_tables_[num_threads_];
  std::shared_ptr<std::vector<AggKeyType>> partitioned_keys_[num_threads_][num_threads_];
  std::shared_ptr<storage::AbstractTable> output_tables_[num_threads_];


  void ParallelAggregatorThread(size_t tid, std::shared_ptr<LogicalTile> tile);
  static size_t ChunkRange(size_t num_tuples, size_t tid);
  void CombineEntries(AggregateList *new_entry, AggregateList *local_entry);
  std::atomic<int> arrival_count_;
  std::atomic<bool> phase_1_completed_;
  std::atomic<bool> phase_2_completed_;
};

}  // namespace executor
}  // namespace peloton
