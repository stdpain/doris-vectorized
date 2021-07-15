#include "vec/exec/join/vhash_join_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
// now we only support inner join
HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _hash_table_rows(0) {}

HashJoinNode::~HashJoinNode() {}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;

    for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
        VExprContext* ctx = NULL;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjuncts[i].left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjuncts[i].right, &ctx));
        _build_expr_ctxs.push_back(ctx);
        if (eq_join_conjuncts[i].__isset.opcode &&
            eq_join_conjuncts[i].opcode == TExprOpcode::EQ_FOR_NULL) {
            _is_null_safe_eq_join.push_back(true);
        } else {
            _is_null_safe_eq_join.push_back(false);
        }
    }

    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, tnode.hash_join_node.other_join_conjuncts,
                                             &_other_join_conjunct_ctxs));

    if (!_other_join_conjunct_ctxs.empty()) {
        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        _build_unique = false;
    }

    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    // Build phase
    auto build_phase_profile = runtime_profile()->create_child("BuildPhase", true, true);
    runtime_profile()->add_child(build_phase_profile, false, nullptr);
    _build_timer = ADD_TIMER(build_phase_profile, "BuildTime");
    _build_table_timer = ADD_TIMER(build_phase_profile, "BuildTableTime");
    _build_hash_calc_timer = ADD_TIMER(build_phase_profile, "BuildHashCalcTime");
    _build_bucket_calc_timer = ADD_TIMER(build_phase_profile, "BuildBucketCalcTime");
    _build_expr_call_timer = ADD_TIMER(build_phase_profile, "BuildExprCallTime");
    _build_table_insert_timer = ADD_TIMER(build_phase_profile, "BuildTableInsertTime");
    _build_table_spread_timer = ADD_TIMER(build_phase_profile, "BuildTableSpreadTime");
    _build_acquire_block_timer = ADD_TIMER(build_phase_profile, "BuildAcquireBlockTime");
    _build_rows_counter = ADD_COUNTER(build_phase_profile, "BuildRows", TUnit::UNIT);

    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");

    // Probe phase
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_timer = ADD_TIMER(probe_phase_profile, "ProbeTime");
    _probe_rows_counter = ADD_COUNTER(probe_phase_profile, "ProbeRows", TUnit::UNIT);
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
    _hash_tbl_load_factor_counter =
            ADD_COUNTER(runtime_profile(), "LoadFactor", TUnit::DOUBLE_VALUE);

    RETURN_IF_ERROR(
            VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc(), expr_mem_tracker()));
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    RETURN_IF_ERROR(
            VExpr::prepare(_other_join_conjunct_ctxs, state, _row_descriptor, expr_mem_tracker()));

    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();
    _build_tuple_size = num_build_tuples;
    _build_tuple_idx.reserve(num_build_tuples);

    for (int i = 0; i < _build_tuple_size; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        _build_tuple_idx.push_back(_row_descriptor.get_tuple_idx(build_tuple_desc->id()));
    }

    // right table data types
    auto right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    // Hash Table Init

    DataTypes hash_table_value_types;
    hash_table_value_types.reserve(right_table_data_types.size() + _build_expr_ctxs.size());
    // TODO: for build expr, we don't have to generate new column if expr is slot reference

    // make build expr value to first column
    for (int32_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        hash_table_value_types.emplace_back(_build_expr_ctxs[i]->root()->data_type());
    }
    for (int32_t i = 0; i < right_table_data_types.size(); ++i) {
        hash_table_value_types.emplace_back(std::move(right_table_data_types[i]));
    }

    _hash_table.init_values(std::move(hash_table_value_types));
    // reserve hash table size
    _hash_table.reserve_size(8388608);

    // Some Function Init
    auto data_type_u64 = std::make_shared<DataTypeUInt64>();

    ColumnsWithTypeAndName hash_func_params;
    std::transform(_build_expr_ctxs.begin(), _build_expr_ctxs.end(),
                   std::back_inserter(hash_func_params), [](auto& expr) -> ColumnWithTypeAndName {
                       return {nullptr, expr->root()->data_type(), ""};
                   });

    const char* hash_func_name = "murmurHash2_64";
    _hash_func = SimpleFunctionFactory::instance().get_function(hash_func_name, hash_func_params,
                                                                data_type_u64);
    LOG_IF(FATAL, (_hash_func == nullptr))
            << fmt::format("couldn't found a function: {}", hash_func_name);

    ColumnsWithTypeAndName mod_func_params = {{nullptr, data_type_u64, "hash_val"},
                                              {nullptr, data_type_u64, "mod"}};

    const char* mod_func_name = "mod";
    _mod_func = SimpleFunctionFactory::instance().get_function(mod_func_name, mod_func_params,
                                                               data_type_u64);
    LOG_IF(FATAL, (_hash_func == nullptr))
            << fmt::format("couldn't found a function: {}", mod_func_name);
    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented Aggregation Node::get_next scalar");
}
Status HashJoinNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    block->clear();
    *eos = true;
    return Status::OK();
}

Status HashJoinNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_other_join_conjunct_ctxs, state));

    RETURN_IF_ERROR(hash_table_build(state));

    return Status::OK();
}

Status HashJoinNode::hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(1)->open(state));
    SCOPED_TIMER(_build_timer);
    Block block;

    bool eos = false;
    while (!eos) {
        block.clear();
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(1)->get_next(state, &block, &eos));
        RETURN_IF_ERROR(_acquire_block(block));
    }
    RETURN_IF_ERROR(_process_build_blocks());
    // RETURN_IF_ERROR(process_build_block(block));
    return Status::OK();
}

Status HashJoinNode::_process_build_blocks() {
    for (auto& block : _block_list) {
        RETURN_IF_ERROR(_process_build_block(block));
        // TODO release block
    }
    return Status::OK();
}

Status HashJoinNode::_process_build_block(Block& block) {
    SCOPED_TIMER(_build_table_timer);
    // process block
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }
    _group_id_vec.resize(rows);
    _bucket_vec.resize(rows);

    // cacaulate hashValueV
    size_t build_column_sz = _build_expr_ctxs.size();
    //
    _build_column_numbers.resize(build_column_sz);

    ColumnNumbers build_column_numbers(build_column_sz);
    // Call Build Expr
    {
        SCOPED_TIMER(_build_expr_call_timer);
        for (size_t i = 0; i < build_column_sz; ++i) {
            int result_col_id = -1;
            RETURN_IF_ERROR(_build_expr_ctxs[i]->execute(&block, &result_col_id));
            build_column_numbers[i] = result_col_id;
        }
    }

    int result_id = block.columns();
    {
        SCOPED_TIMER(_build_hash_calc_timer);
        block.insert({nullptr, std::make_shared<DataTypeUInt64>(), "hash_val"});
        _hash_func->execute(block, build_column_numbers, result_id, rows);
    }
    // modulo hash value to acqure bucket
    auto hash_column = block.get_by_position(result_id).column;
    auto& hash_val_data = assert_cast<const ColumnUInt64*>(hash_column.get())->get_data();
    for (int i = 0; i < rows; ++i) {
        _bucket_vec[i] = hash_val_data[i] & (_hash_table.bucket_size() - 1);
    }

    // prepare
    _build_columns.resize(_hash_table.data_types().size());
    for (int i = 0; i < build_column_numbers.size(); ++i) {
        _build_columns[i] = block.get_by_position(build_column_numbers[i]).column;
    }
    // assign map child(1) to here
    for (int i = 0; i < _hash_table.data_types().size() - build_column_sz; ++i) {
        _build_columns[i + build_column_sz] = block.get_by_position(i).column;
    }
    // Insert Hash Table
    {
        SCOPED_TIMER(_build_table_insert_timer);
        _hash_table.insert(_group_id_vec, _bucket_vec, rows);
        {
            SCOPED_TIMER(_build_table_spread_timer);
            _hash_table.spread(_group_id_vec, _build_columns, rows);
        }
    }
    return Status::OK();
}

Status HashJoinNode::_acquire_block(Block& block) {
    SCOPED_TIMER(_build_acquire_block_timer);
    size_t rows = block.rows();
    _block_list.emplace_back(std::move(block));
    _hash_table_rows += rows;
    COUNTER_SET(_build_rows_counter, _hash_table_rows);
    return Status::OK();
}
} // namespace doris::vectorized