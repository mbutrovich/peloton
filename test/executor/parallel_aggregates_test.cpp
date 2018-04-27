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

TEST_F(ParallelAggregatesTests, HashSumGroupByTest) {
  // SELECT b, SUM(c) from table GROUP BY b;
//  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and wrap it in logical tiles
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      TestingExecutorUtil::CreateTable(1000, false));
  TestingExecutorUtil::PopulateTable(data_table.get(), 1000000, false,
                                   true, true, txn);
  txn_manager.CommitTransaction(txn);

  std::unique_ptr<executor::LogicalTile> source_logical_tile1(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0)));

  std::unique_ptr<executor::LogicalTile> source_logical_tile2(
      executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1)));

  std::vector<std::unique_ptr<executor::LogicalTile>> tile_vec;

//  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(0))));
//  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor::LogicalTileFactory::WrapTileGroup(data_table->GetTileGroup(1))));
//
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
      .WillOnce(Return(true))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput())
      .WillOnce(Return(source_logical_tile1.release()))
      .WillOnce(Return(source_logical_tile2.release()));

  EXPECT_TRUE(executor.Init());

  EXPECT_TRUE(executor.Execute());

  txn_manager.CommitTransaction(txn);

  // Verify result
//  std::unique_ptr<executor::LogicalTile> result_tile(executor.GetOutput());

  tile_vec.clear();
  tile_vec.push_back(std::unique_ptr<executor::LogicalTile>(executor.GetOutput()));
  std::cout << TestingExecutorUtil::GetTileVectorInfo(tile_vec);
  // FIXME This should pass
  //  EXPECT_GE(3, result_tile->GetTupleCount());
}

}  // namespace test
}  // namespace peloton
