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

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class ParallelAggregatesTests : public PelotonTest {};

constexpr size_t num_tuples = 50000;

TEST_F(ParallelAggregatesTests, SequentialHashSumGroupByTest) {
  // SELECT b, SUM(c) from table GROUP BY b;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(num_tuples, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), num_tuples, false,
                                   true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

//  std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;

//  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0))));

//  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec) << std::endl;

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

  auto start = std::chrono::system_clock::now();
  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());
  auto end = std::chrono::system_clock::now();

  txn_manager.CommitTransaction(txn);

  std::chrono::duration<double> elapsed_seconds = end-start;
  std::cout << "sequential aggregation elapsed time: " << elapsed_seconds.count() << "s\n";

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());

//  tile_vec.clear();
//  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor.GetOutput()));
//  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec);
}

TEST_F(ParallelAggregatesTests, ParallelHashSumGroupByTest) {
  // SELECT b, SUM(c) from table GROUP BY b;
//  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(num_tuples, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), num_tuples, false,
                                     true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

//  std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;

//  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0))));

//  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec) << std::endl;

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


  auto start = std::chrono::system_clock::now();
  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());
  auto end = std::chrono::system_clock::now();

  txn_manager.CommitTransaction(txn);

  std::chrono::duration<double> elapsed_seconds = end-start;
  std::cout << "parallel aggregation elapsed time: " << elapsed_seconds.count() << "s\n";

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());

//  tile_vec.clear();
//  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor.GetOutput()));
//  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec);
}

TEST_F(ParallelAggregatesTests, ParallelHashCountDistinctGroupByTest) {
  // SELECT a, COUNT(b), COUNT(DISTINCT b) from table group by a
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

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

  txn_manager.CommitTransaction(txn);

  // Verify result
  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());
  EXPECT_TRUE(result_tile.get() != nullptr);
  type::Value val = (result_tile->GetValue(0, 0));
  CmpBool cmp =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(0)));
  CmpBool cmp1 =
      (val.CompareEquals(type::ValueFactory::GetIntegerValue(10)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue || cmp1 == CmpBool::CmpTrue);

  val = (result_tile->GetValue(0, 1));
  cmp = (val.CompareEquals(type::ValueFactory::GetIntegerValue(5)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue);

  val = (result_tile->GetValue(0, 2));
  cmp = (val.CompareLessThanEquals(type::ValueFactory::GetIntegerValue(3)));
  EXPECT_TRUE(cmp == CmpBool::CmpTrue);
}

TEST_F(ParallelAggregatesTests, ParallelHashMinMaxTest) {
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
