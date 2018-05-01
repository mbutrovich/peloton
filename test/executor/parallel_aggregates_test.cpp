//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_aggregates_test.cpp
//
// Identification: test/executor/parallel_aggregates_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <chrono>

#include "executor/testing_executor_util.h"
#include "common/harness.h"

#include "common/internal_types.h"
#include "type/value.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/aggregate_executor.h"
#include "executor/executor_context.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "expression/expression_util.h"
#include "planner/abstract_plan.h"
#include "planner/aggregate_plan.h"
#include "storage/data_table.h"
#include "executor/mock_executor.h"

#include <algorithm>
#include <cmath>
#include <random>


using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class ParallelAggregatesTests : public PelotonTest {};

// Benchmark Parameters
constexpr size_t benchmark_num_tuples = 1000000;

//bool benchmark_sequential = true;
bool benchmark_sequential = false;
//bool benchmark_parallel = false;
bool benchmark_parallel = true;

bool print_result_table = false;
bool print_time_components = true;

// Scenarios

// zipf with low q
//bool uniform = false;
//int constant = 0;
//double q = 1.2;
//int group_range = 10000000;

// uniform across groups
bool uniform = true;
int constant = 0;
double q = 0.0;
int group_range = 100000;

// uniform across distinct groups
//bool uniform = true;
//int constant = 0;
//double q = 0.0;
//int group_range = benchmark_num_tuples;

// all the same group
//bool uniform = false;
//int constant = 1;
//double q = 0.0;
//int group_range = 9999;

void PrintRuntimeInfoParallel(executor::AggregateExecutor &executor) {
  std::cout << std::endl;
  std::cout << "======== TIMING INFO (Parallel) --======================" << std::endl;
  double total = 0.0;
  for (size_t i=0; i < executor.num_phases_; i++) {
    double max = 0.0;

    if (print_time_components) {
      std::cout << "Phase " << i << ": ";
    }

    for (size_t j = 0; j < executor.num_threads_; j++) {
      double time_elapsed = executor.timers_[i][j];

      if (print_time_components) {
        std::cout << "T" << j << " " << time_elapsed << " ";
      }

      max = (time_elapsed > max) ? time_elapsed : max;
    }

    if (print_time_components) {
      std::cout << std::endl;
      std::cout << "Phase " << i << " Max = " << max << std::endl;
    }

    total += max;
  }
  std::cout << "======== Total Time ========================================" << std::endl;
  std::cout << total << " ms" << std::endl;
  std::cout << "============================================================" << std::endl;
}

void PrintRuntimeInfoSequential(executor::AggregateExecutor &executor) {
  std::cout << std::endl;
  std::cout << "======== TIMING INFO (Sequential) --=====================" << std::endl;
  double total = 0;
  for (size_t i=0; i < executor.num_phases_; i++) {
    double time_elapsed = executor.timers_[i][0];

    if (print_time_components) {
      std::cout << "Phase " << i << " = " << time_elapsed << std::endl;
    }

    total += time_elapsed;
  }
  std::cout << "======== Total Time ========================================" << std::endl;
  std::cout << total << " ms" << std::endl;
  std::cout << "============================================================" << std::endl;
  std::cout << std::endl;
}

//TEST_F(ParallelAggregatesTests, NaivePartitioningTest) {
//  float min_val = 0.0f;
//  float max_val = 101.0f;
//  size_t num_threads_ = 10;
//  float val;
//  size_t partition;
//
//  val = 2.0;
//  partition = (size_t)(((val-min_val)/max_val) * num_threads_);
//  EXPECT_EQ(0, partition);
//
//  val = 11.0;
//  partition = (size_t)(((val-min_val)/max_val) * num_threads_);
//  EXPECT_EQ(1, partition);
//
//  val = 33.0;
//  partition = (size_t)(((val-min_val)/max_val) * num_threads_);
//  EXPECT_EQ(3, partition);
//
//  val = 67.0;
//  partition = (size_t)(((val-min_val)/max_val) * num_threads_);
//  EXPECT_EQ(6, partition);
//
//  val = 100.0;
//  partition = (size_t)(((val-min_val)/max_val) * num_threads_);
//  EXPECT_EQ(9, partition);
//}

TEST_F(ParallelAggregatesTests, HashSumGroupByBenchmark) {
  // SELECT b, SUM(c) from table GROUP BY b;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(benchmark_num_tuples, false));


  TestingExecutorUtil::PopulateTableCustom(data_table.get(),
                                           benchmark_num_tuples,
                                           uniform,
                                           constant,
                                           q,
                                           group_range,
                                           txn);
  txn_manager.CommitTransaction(txn);

  if (benchmark_sequential) {

    std::unique_ptr<executor::LogicalTile> source_logical_tile1(
        executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

    // (1-5) Setup plan node

    // 1) Set up group-by columns
    std::vector<oid_t> group_by_columns = {1};

    // 2) Set up project info
    DirectMapList direct_map_list = {{0, {0, 1}}, {1, {1, 0}}};

    std::unique_ptr<const planner::ProjectInfo> proj_info(
        new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

    // 3) Set up unique aggregates
    std::vector<planner::AggregatePlan::AggTerm> agg_terms;
    planner::AggregatePlan::AggTerm sumC(
        ExpressionType::AGGREGATE_SUM,
        expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                      2));
    agg_terms.push_back(sumC);

    // 4) Set up predicate (empty)
    std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

    // 5) Create output table schema
    auto data_table_schema = data_table.get()->GetSchema();
    std::vector<oid_t> set = {1, 2};
    std::vector<catalog::Column> columns;
    for (auto column_index : set) {
      columns.push_back(data_table_schema->GetColumn(column_index));
    }
    std::shared_ptr<const catalog::Schema> output_table_schema(
        new catalog::Schema(columns));

    // OK) Create the plan node
    planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                                std::move(agg_terms), std::move(group_by_columns),
                                output_table_schema, AggregateType::HASH);

    // Create and set up executor
    txn = txn_manager.BeginTransaction();
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(txn));

    executor::AggregateExecutor *executor = new executor::AggregateExecutor(&node, context.get());
    MockExecutor child_executor;
    executor->AddChild(&child_executor);

    EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

    EXPECT_CALL(child_executor, DExecute())
        .WillOnce(Return(true));

    EXPECT_CALL(child_executor, GetOutput())
        .WillOnce(Return(source_logical_tile1.release()));

    EXPECT_TRUE(executor->Init());

    std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;
    while (executor->Execute()) {
      tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor->GetOutput()));
    }
    txn_manager.CommitTransaction(txn);

    if (print_result_table) {
      std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec);
    }

    tile_vec.clear();

    PrintRuntimeInfoSequential(*executor);
    delete executor;
    context.release();
  }

  ///////////////////////// END SEQUENTIAL ////////////////////////////////////////

  ///////////////////////// START PARALLEL ////////////////////////////////////////

  if (benchmark_parallel) {
    std::unique_ptr<executor::LogicalTile> source_logical_tile2(
        executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

    // (1-5) Setup plan node

    // 1) Set up group-by columns
    std::vector<oid_t> group_by_columns2 = {1};

    // 2) Set up project info
    DirectMapList direct_map_list2 = {{0, {0, 1}}, {1, {1, 0}}};

    std::unique_ptr<const planner::ProjectInfo> proj_info2(
        new planner::ProjectInfo(TargetList(), std::move(direct_map_list2)));

    // 3) Set up unique aggregates
    std::vector<planner::AggregatePlan::AggTerm> agg_terms2;
    planner::AggregatePlan::AggTerm sumC2(
        ExpressionType::AGGREGATE_SUM,
        expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                      2));
    agg_terms2.push_back(sumC2);

    // 4) Set up predicate (empty)
    std::unique_ptr<const expression::AbstractExpression> predicate2(nullptr);

    // 5) Create output table schema
    auto data_table_schema2 = data_table.get()->GetSchema();
    std::vector<oid_t> set2 = {1, 2};
    std::vector<catalog::Column> columns2;
    for (auto column_index : set2) {
      columns2.push_back(data_table_schema2->GetColumn(column_index));
    }
    std::shared_ptr<const catalog::Schema> output_table_schema2(
        new catalog::Schema(columns2));

    // OK) Create the plan node
    planner::AggregatePlan node2(std::move(proj_info2), std::move(predicate2),
                                std::move(agg_terms2), std::move(group_by_columns2),
                                output_table_schema2, AggregateType::PARALLEL_HASH);

    // Create and set up executor
    auto txn2 = txn_manager.BeginTransaction();
    std::unique_ptr<executor::ExecutorContext> context2(
        new executor::ExecutorContext(txn2));

    executor::AggregateExecutor executor2(&node2, context2.get());
    MockExecutor child_executor2;
    executor2.AddChild(&child_executor2);

    EXPECT_CALL(child_executor2, DInit()).WillOnce(Return(true));

    EXPECT_CALL(child_executor2, DExecute())
        .WillOnce(Return(true));

    EXPECT_CALL(child_executor2, GetOutput())
        .WillOnce(Return(source_logical_tile2.release()));

    EXPECT_TRUE(executor2.Init());

    std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec2;
    while(executor2.Execute()) {
      tile_vec2.push_back(std::unique_ptr<executor::LogicalTile>(executor2.GetOutput()));
    }
    txn_manager.CommitTransaction(txn2);

    if (print_result_table) {
      std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec2);
    }

    PrintRuntimeInfoParallel(executor2);
  }
}

TEST_F(ParallelAggregatesTests, DISABLED_SequentialHashSumGroupByTest) {
  // SELECT b, SUM(c) from table GROUP BY b;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(2*tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2*tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;

  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0))));

  std::cout << std::endl << TestingExecutorUtil::GetTileVectorInfo(tile_vec) << std::endl;

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {1};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 1}}, {1, {1, 0}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumC(
      ExpressionType::AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2));
  agg_terms.push_back(sumC);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {1, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::AggregateExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);

  // Verify result
//  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());

  tile_vec.clear();
  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor.GetOutput()));
  std::cout << std::endl << TestingExecutorUtil::GetTileVectorInfo(tile_vec) << std::endl;
}

TEST_F(ParallelAggregatesTests, DISABLED_ParallelHashSumGroupByTest) {
  // SELECT b, SUM(c) from table GROUP BY b;
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(2*tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2*tuple_count, false,
                                     false, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;

  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0))));

  std::cout << std::endl << TestingExecutorUtil::GetTileVectorInfo(tile_vec) << std::endl;

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {1};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 1}}, {1, {1, 0}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm sumC(
      ExpressionType::AGGREGATE_SUM,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2));
  agg_terms.push_back(sumC);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {1, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::PARALLEL_HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::AggregateExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_TRUE(executor.Init());

  tile_vec.clear();
  while(executor.Execute()) {
    tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor.GetOutput()));
  }

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec);
}

TEST_F(ParallelAggregatesTests, DISABLED_ParallelHashCountDistinctGroupByTest) {
  // SELECT a, COUNT(b), COUNT(DISTINCT b) from table group by a
  const int tuple_count = 5000;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(2 * tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                     true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns = {0};

  // 2) Set up project info
  DirectMapList direct_map_list = {{0, {0, 0}}, {1, {1, 0}}, {2, {1, 1}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm countB(
      ExpressionType::AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);  // Flag distinct
  planner::AggregatePlan::AggTerm countDistinctB(
      ExpressionType::AGGREGATE_COUNT,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      true);  // Flag distinct
  agg_terms.push_back(countB);
  agg_terms.push_back(countDistinctB);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {0, 1, 1};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::PARALLEL_HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::AggregateExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;
  while(executor.Execute()) {
    tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor.GetOutput()));
  }
  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec);

  txn_manager.CommitTransaction(txn);


  // Verify result
//  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
//  EXPECT_TRUE(result_tile.get() != nullptr);
//  type::Value val = (result_tile->GetValue(0, 0));
//  CmpBool cmp =
//      (val.CompareEquals(type::ValueFactory::GetIntegerValue(0)));
//  CmpBool cmp1 =
//      (val.CompareEquals(type::ValueFactory::GetIntegerValue(10)));
//  EXPECT_TRUE(cmp == CmpBool::CmpTrue || cmp1 == CmpBool::CmpTrue);
//
//  val = (result_tile->GetValue(0, 1));
//  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(5)));
//  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
//
//  val = (result_tile->GetValue(0, 2));
//  cmp = (val.CompareLessThanEquals(type::ValueFactory::GetIntegerValue(3)));
//  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
}

TEST_F(ParallelAggregatesTests, DISABLED_ParallelHashMinMaxTest) {
  // SELECT MIN(b), MAX(b), MIN(c), MAX(c) from table
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(2 * tuple_count, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 2 * tuple_count, false,
                                     false, false, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  // (1-5) Setup plan node

  // 1) Set up group-by columns
  std::vector<oid_t> group_by_columns;

  // 2) Set up project info
  DirectMapList direct_map_list = {
      {0, {1, 0}}, {1, {1, 1}}, {2, {1, 2}}, {3, {1, 3}}};

  std::unique_ptr<const planner::ProjectInfo> proj_info(
      new planner::ProjectInfo(TargetList(), std::move(direct_map_list)));

  // 3) Set up unique aggregates
  std::vector<planner::AggregatePlan::AggTerm> agg_terms;
  planner::AggregatePlan::AggTerm minB(
      ExpressionType::AGGREGATE_MIN,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);
  planner::AggregatePlan::AggTerm maxB(
      ExpressionType::AGGREGATE_MAX,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::INTEGER, 0,
                                                    1),
      false);
  planner::AggregatePlan::AggTerm minC(
      ExpressionType::AGGREGATE_MIN,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2),
      false);
  planner::AggregatePlan::AggTerm maxC(
      ExpressionType::AGGREGATE_MAX,
      expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
                                                    2),
      false);
  agg_terms.push_back(minB);
  agg_terms.push_back(maxB);
  agg_terms.push_back(minC);
  agg_terms.push_back(maxC);

  // 4) Set up predicate (empty)
  std::unique_ptr<const expression::AbstractExpression> predicate(nullptr);

  // 5) Create output table schema
  auto data_table_schema = data_table.get()->GetSchema();
  std::vector<oid_t> set = {1, 1, 2, 2};
  std::vector<catalog::Column> columns;
  for (auto column_index : set) {
    columns.push_back(data_table_schema->GetColumn(column_index));
  }
  std::shared_ptr<const catalog::Schema> output_table_schema(
      new catalog::Schema(columns));

  // OK) Create the plan node
  planner::AggregatePlan node(std::move(proj_info), std::move(predicate),
                              std::move(agg_terms), std::move(group_by_columns),
                              output_table_schema, AggregateType::PARALLEL_HASH);

  // Create and set up executor
  txn = txn_manager.BeginTransaction();
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  executor::AggregateExecutor executor(&node, context.get());
  MockExecutor child_executor;
  executor.AddChild(&child_executor);

  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));

  EXPECT_CALL(child_executor, DExecute())
      .WillOnce(Return(true));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(1)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(91)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
  val = (result_tile->GetValue(0, 2));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(2)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
  val = (result_tile->GetValue(0, 3));
  cmp = (val.CompareEquals(type::ValueFactory::GetDecimalValue(92)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
}

}  // namespace test
}  // namespace peloton
