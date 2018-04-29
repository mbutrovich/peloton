//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_executor.cpp
//
// Identification: src/executor/aggregate_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container_tuple.h"
#include "common/logger.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/aggregate_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile_factory.h"
#include "planner/aggregate_plan.h"
#include "storage/table_factory.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for aggregate executor.
 * @param node Aggregate node corresponding to this executor.
 */
AggregateExecutor::AggregateExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

AggregateExecutor::~AggregateExecutor() {
  // clean up temporary aggregation table
  delete output_table;
}

/**
 * @brief Basic initialization.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DInit() {
  PELOTON_ASSERT(children_.size() == 1);

  LOG_TRACE("Aggregate executor :: 1 child ");

  // Grab info from plan node and check it
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Construct the output table
  auto output_table_schema =
      const_cast<catalog::Schema *>(node.GetOutputSchema());

  PELOTON_ASSERT(output_table_schema->GetColumnCount() >= 1);

  // clean up result
  result_itr = START_OID;
  result.clear();

  // reset done
  done = false;

  // clean up temporary aggregation table
  delete output_table;

  output_table =
      storage::TableFactory::GetTempTable(output_table_schema, false);

  return true;
}

/**
 * @brief Creates logical tile(s) wrapping the results of aggregation.
 * @return true on success, false otherwise.
 */
bool AggregateExecutor::DExecute() {
  // Already performed the aggregation
  if (done) {
    if (result_itr == INVALID_OID || result_itr == result.size()) {
      return false;
    } else {
      // Return appropriate tile and go to next tile
      SetOutput(result[result_itr]);
      result_itr++;
      return true;
    }
  }

  // Initialize the aggregator
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  if (node.GetAggregateStrategy() == AggregateType::HASH) {

    LOG_TRACE("Use Sequential Hash");
    return DExecuteSequential();

  } else if (node.GetAggregateStrategy() == AggregateType::PARALLEL_HASH) {

    LOG_TRACE("Use Parallel Hash");
    return DExecuteParallel();

  } else {

    LOG_ERROR("Invalid aggregate type. Return.");
    return false;

  }
}

bool AggregateExecutor::DExecuteSequential() {
  auto start = std::chrono::system_clock::now();
  // Grab info from plan node
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Get input tile and aggregate them
  children_[0]->Execute();
  std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

  // Initialize the aggregator
  std::unique_ptr<AbstractAggregator> aggregator(new HashAggregator(
    &node, output_table, executor_context_, tile->GetColumnCount()));

  LOG_TRACE("Looping over tile..");
  for (oid_t tuple_id : *tile) {
    std::unique_ptr<ContainerTuple<LogicalTile>> cur_tuple(
        new ContainerTuple<LogicalTile>(tile.get(), tuple_id));

    if (aggregator->Advance(cur_tuple.get()) == false) {
      return false;
    }
  }
  LOG_TRACE("Finished processing logical tile");
  timers_[0][0] = std::chrono::system_clock::now() - start;

  start = std::chrono::system_clock::now();
  LOG_TRACE("Finalizing..");
  if (!aggregator.get() || !aggregator->Finalize()) {
    // If there's no tuples and no group-by, count() aggregations should return
    // 0 according to the test in MySQL.
    // TODO: We only checked whether all AggTerms are counts here. If there're
    // mixed terms, we should return 0 for counts and null for others.
    bool all_count_aggs = true;
    for (oid_t aggno = 0; aggno < node.GetUniqueAggTerms().size(); aggno++) {
      auto agg_type = node.GetUniqueAggTerms()[aggno].aggtype;
      if (agg_type != ExpressionType::AGGREGATE_COUNT &&
        agg_type != ExpressionType::AGGREGATE_COUNT_STAR)
        all_count_aggs = false;
    }

    // If there's no tuples in the table and only if no group-by in the
    // query, we should return a NULL tuple
    // this is required by SQL
    if (!aggregator.get() && node.GetGroupbyColIds().empty()) {
      LOG_TRACE(
        "No tuples received and no group-by. Should insert a NULL tuple "
        "here.");
      std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(output_table->GetSchema(), true));
      if (all_count_aggs == true) {
        tuple->SetAllZeros();
      } else {
        tuple->SetAllNulls();
      }
      UNUSED_ATTRIBUTE auto location = output_table->InsertTuple(tuple.get());
      PELOTON_ASSERT(location.block != INVALID_OID);
    } else {
      done = true;
      return false;
    }
  }

  // Transform output table into result
  LOG_TRACE("%s", output_table->GetInfo().c_str());
  auto tile_group_count = output_table->GetTileGroupCount();
  if (tile_group_count == 0 || output_table->GetTupleCount() == 0) {
    return false;
  }

  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
  	tile_group_itr++) {
  	auto tile_group = output_table->GetTileGroup(tile_group_itr);
  	PELOTON_ASSERT(tile_group != nullptr);
  	LOG_TRACE("\n%s", tile_group->GetInfo().c_str());

  	// Get the logical tiles corresponding to the given tile group
  	auto logical_tile = LogicalTileFactory::WrapTileGroup(tile_group);

  	result.push_back(logical_tile);
  }
  LOG_TRACE("%s", result[result_itr]->GetInfo().c_str());

  done = true;

  SetOutput(result[result_itr]);
  result_itr++;

  timers_[1][0] = std::chrono::system_clock::now() - start;

  return true;
}

size_t AggregateExecutor::ChunkRange(size_t num_tuples, size_t tid) {
	size_t base = num_tuples / num_threads_;
	size_t extra = num_tuples % num_threads_;
	if (tid < extra)
		return tid * (base + 1);
	else
		return tid * base + extra;
}

void AggregateExecutor::CombineEntries(AggregateList *new_entry,
                                       AggregateList *local_entry) {
  // Grab info from plan node
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();
  for (size_t i=0; i < node.GetUniqueAggTerms().size(); i++) {
    if (node.GetUniqueAggTerms()[i].distinct) {
      for (auto &item : local_entry->aggregates[i]->distinct_set_) {
        new_entry->aggregates[i]->distinct_set_.insert(item);
      }
    }
    else {
      new_entry->aggregates[i]->DCombine(local_entry->aggregates[i]);
    }
  }
}


void AggregateExecutor::ParallelAggregatorThread(size_t my_tid, std::shared_ptr<LogicalTile> tile) {

  auto start = std::chrono::system_clock::now();
  // Grab info from plan node
  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Construct the output table
  auto output_table_schema =
      const_cast<catalog::Schema *>(node.GetOutputSchema());

  // Phase 1 //////////////////////////////////////////////////
  output_tables_[my_tid].reset(storage::TableFactory::GetTempTable(output_table_schema, false));
  local_hash_tables_[my_tid] = std::make_shared<HashAggregateMapType>();
  global_hash_tables_[my_tid] = std::make_shared<HashAggregateMapType>();

  for (size_t partition = 0; partition < num_threads_; partition++) {
    partitioned_keys_[my_tid][partition] = std::make_shared<std::vector<AggKeyType>>();
  }

  // create a local hash table of group by keys
  std::unique_ptr<ParallelHashAggregator> aggregator(
    new ParallelHashAggregator(&node,
                               output_tables_[my_tid].get(),
                               executor_context_,
                               tile->GetColumnCount(),
                               local_hash_tables_[my_tid],
                               partitioned_keys_[my_tid],
                               num_threads_)
  );

  // hash each tuple by its group by key to compute aggregates
  LOG_TRACE("TID %lu: Looping over tile..", tid);
  size_t tuple_count = tile->GetTupleCount();
  size_t tuple_id_start = ChunkRange(tuple_count, my_tid);
  size_t tuple_id_end = ChunkRange(tuple_count, my_tid + 1);
  for (size_t tuple_id = tuple_id_start; tuple_id < tuple_id_end; tuple_id++) {
    std::unique_ptr<ContainerTuple<LogicalTile>> cur_tuple(new ContainerTuple<LogicalTile>(tile.get(), tuple_id));

    aggregator->Advance(cur_tuple.get());
    // note: aggregator will store unique keys in partitioned keys
  }
  LOG_TRACE("Finished processing logical tile");

  timers_[0][my_tid] = std::chrono::system_clock::now() - start;
  // End Phase 1 //////////////////////////////////////////////////

  // Barrier 1: to ensure all threads wait until phase 1 is complete
  start = std::chrono::system_clock::now();

  int prev_arrivals = arrival_count_.fetch_add(1);
  if (prev_arrivals == num_threads_ - 1) {
    arrival_count_.store(0);
    phase_1_completed_.store(true);
  }

  while(phase_1_completed_.load() == false);
  timers_[1][my_tid] = std::chrono::system_clock::now() - start;

  //////////////////////////////////////////////////////////////////
  // Phase 2
  ///////////////////////////////////////////////////////////////////
  start = std::chrono::system_clock::now();

  // declare this thread's global hash table (a partition of the whole keyspace)
  HashAggregateMapType *my_global_hash_table = global_hash_tables_[my_tid].get();

  // for each list of unique keys found by worker threads
  for (size_t list_tid = 0; list_tid < num_threads_; list_tid++) {
    std::vector<AggKeyType> *keys = partitioned_keys_[list_tid][my_tid].get();
    for (auto &key : *keys) {
      // if we haven't seen this key before
      if (my_global_hash_table->count(key) == 0) {
        AggregateList *new_entry = nullptr;

        // merge the local hash table entries
        for (size_t agg_tid = 0; agg_tid < num_threads_; agg_tid++) {

          // combine entries
          auto local_entry = local_hash_tables_[agg_tid]->find(key);
          if (local_entry != local_hash_tables_[agg_tid]->end()) {
            if (new_entry == nullptr) {
              new_entry = local_entry->second;
            } else {
              CombineEntries(new_entry, local_entry->second);
            }
          }
        }
        (*my_global_hash_table)[key] = new_entry;
      }
    }
  }
  timers_[2][my_tid] = std::chrono::system_clock::now() - start;


  // Phase: Materialization
  start = std::chrono::system_clock::now();

  aggregator->aggregates_map.swap(global_hash_tables_[my_tid]);

  if (!aggregator->Finalize()) {
    output_tables_[my_tid].reset();
    output_tables_[my_tid] = nullptr;
  }
  timers_[3][my_tid] = std::chrono::system_clock::now() - start;

  // Barrier to ensure all threads wait until phase 2 is complete
  start = std::chrono::system_clock::now();

  prev_arrivals = arrival_count_.fetch_add(1);
  if (prev_arrivals == num_threads_ - 1) {
    phase_2_completed_.store(true);
  }

  while(phase_2_completed_.load() == false);
  timers_[4][my_tid] = std::chrono::system_clock::now() - start;
}

bool AggregateExecutor::DExecuteParallel() {
  arrival_count_.store(0);
  phase_1_completed_.store(false);
  phase_2_completed_.store(false);

  const planner::AggregatePlan &node = GetPlanNode<planner::AggregatePlan>();

  // Construct the output table
  auto output_table_schema =
      const_cast<catalog::Schema *>(node.GetOutputSchema());

  children_[0]->Execute();
  std::shared_ptr<LogicalTile> tile(children_[0]->GetOutput());

  // Launch num_threads aggregators to perform the aggregate in parallel
  for (size_t tid = 0; tid < num_threads_; tid++) {
    threads_[tid] = std::thread(&AggregateExecutor::ParallelAggregatorThread, this, tid, tile);
  }

  // wait for all threads to complete their work
  for (size_t tid = 0; tid < num_threads_; tid++) {
    threads_[tid].join();
  }

  bool no_results = true;
  for (size_t tid = 0; tid < num_threads_; tid++) {
    if (output_tables_[tid] != nullptr) {
      no_results = false;
      break;
    }
  }

  LOG_TRACE("Master Finalizing..");
  if (no_results) {
    // If there's no tuples and no group-by, count() aggregations should return
    // 0 according to the test in MySQL.
    // TODO: We only checked whether all AggTerms are counts here. If there're
    // mixed terms, we should return 0 for counts and null for others.
    bool all_count_aggs = true;
    for (oid_t aggno = 0; aggno < node.GetUniqueAggTerms().size(); aggno++) {
      auto agg_type = node.GetUniqueAggTerms()[aggno].aggtype;
      if (agg_type != ExpressionType::AGGREGATE_COUNT &&
        agg_type != ExpressionType::AGGREGATE_COUNT_STAR)
        all_count_aggs = false;
    }

    // If there's no tuples in the table and only if no group-by in the
    // query, we should return a NULL tuple
    // this is required by SQL
    if (node.GetGroupbyColIds().empty()) {
      LOG_TRACE(
        "No tuples received and no group-by. Should insert a NULL tuple "
        "here.");
      std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(output_table->GetSchema(), true));
      if (all_count_aggs == true) {
        tuple->SetAllZeros();
      } else {
        tuple->SetAllNulls();
      }
      output_tables_[0].reset(storage::TableFactory::GetTempTable(output_table_schema, false));
  		UNUSED_ATTRIBUTE auto location = output_tables_[0]->InsertTuple(tuple.get());
  		PELOTON_ASSERT(location.block != INVALID_OID);
  	} else {
      done = true;
      return false;
    }
  }

  // for each table
  		// for each tile_group
  				// wrap tile_group in a logical_tile and push onto result vector
  for (size_t tid = 0; tid < num_threads_; tid++) {
    if (output_tables_[tid] != nullptr) {
      auto tile_group_count = output_tables_[tid]->GetTileGroupCount();
      for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
        tile_group_itr++) {
        auto tile_group = output_tables_[tid]->GetTileGroup(tile_group_itr);
        PELOTON_ASSERT(tile_group != nullptr);
        LOG_TRACE("\n%s", tile_group->GetInfo().c_str());

        // Get the logical tiles corresponding to the given tile group
        auto logical_tile = LogicalTileFactory::WrapTileGroup(tile_group);
        result.push_back(logical_tile);
      }
    }
  }
  LOG_TRACE("%s", result[result_itr]->GetInfo().c_str());

  done = true;

  SetOutput(result[result_itr]);
  result_itr++;

  return true;
}

}  // namespace executor
}  // namespace peloton



