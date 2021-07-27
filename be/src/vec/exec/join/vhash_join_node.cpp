#include "vec/exec/join/vhash_join_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "util/defer_op.h"
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
    _build_table_expanse_timer = ADD_TIMER(build_phase_profile, "BuildTableExpanseTime");
    _build_acquire_block_timer = ADD_TIMER(build_phase_profile, "BuildAcquireBlockTime");
    _build_rows_counter = ADD_COUNTER(build_phase_profile, "BuildRows", TUnit::UNIT);

    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    // Probe phase
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_timer = ADD_TIMER(probe_phase_profile, "ProbeTime");
    _probe_gather_timer = ADD_TIMER(probe_phase_profile, "ProbeGatherTime");
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_diff_timer = ADD_TIMER(probe_phase_profile, "ProbeDiffTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _probe_hash_calc_timer = ADD_TIMER(probe_phase_profile, "ProbeHashCalcTime");
    _probe_select_miss_timer = ADD_TIMER(probe_phase_profile, "ProbeSelectMissTime");
    _probe_select_zero_timer = ADD_TIMER(probe_phase_profile, "ProbeSelectZeroTime");
    _probe_rows_counter = ADD_COUNTER(probe_phase_profile, "ProbeRows", TUnit::UNIT);
    _probe_max_length = runtime_profile()->AddHighWaterMarkCounter("ProbeMaxLength", TUnit::UNIT);
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
    right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());
    // Hash Table Init

    // _hash_table.reset(new MapI32(8388608));
    // _hash_table.reset(new MapI32(2097152));
    // _hash_table.reset(new MapI32(4096));
    // _hash_table.reset(new MapI32(4194304));

    _hash_table.reset(new MapI32());
    // _hash_table.reset(new MapI32(4194304));
    LOG(WARNING) << "========= init bucket size :" << _hash_table->get_buffer_size_in_cells();
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

// TODO: got a iterator
Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    Block probe_block;
    RETURN_IF_ERROR(child(0)->get_next(state, &probe_block, eos));
    SCOPED_TIMER(_probe_timer);

    size_t rows = probe_block.rows();
    if (rows == 0) {
        *eos = true;
        return Status::OK();
    }

    // start probe
    // prepare probe columns
    Sizes sizes(1);
    int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
    ColumnRawPtrs probe_columns(probe_expr_ctxs_sz);
    for (int i = 0; i < probe_expr_ctxs_sz; ++i) {
        int result_id = -1;
        _probe_expr_ctxs[i]->execute(&probe_block, &result_id);
        DCHECK_GE(result_id, 0);
        probe_columns[i] = probe_block.get_by_position(result_id).column.get();
    }

    //
    I32KeyType key_getter(probe_columns, sizes, nullptr);

    // find and build block
    MutableBlock mutable_block(VectorizedUtils::create_empty_columnswithtypename(row_desc()));
    output_block->clear();
    IColumn::Offsets offset_data;
    auto& mcol = mutable_block.mutable_columns();

    int right_col_idx = left_table_data_types.size();
    auto& dst_col = assert_cast<ColumnInt32*>(mcol[right_col_idx].get())->get_data();
    int current_offset = 0;

    for (size_t i = 0; i < rows; ++i) {
        auto find_result = key_getter.find_key(*_hash_table, i, _arena);
        if (find_result.is_found()) {
            auto& mapped = find_result.get_mapped();
            current_offset += mapped.value_sz;
            for (int j = 0; j < mapped.value_sz; ++j) {
                dst_col.push_back(key_getter.get_key_holder(i, _arena));
            }
        }

        offset_data.push_back(current_offset);
    }

    output_block->swap(mutable_block.to_block());
    for (int i = 0; i < right_col_idx; ++i) {
        auto& column = probe_block.get_by_position(i).column;
        output_block->get_by_position(i).column = column->replicate(offset_data);
    }

    int m = dst_col.size();
    COUNTER_UPDATE(_rows_returned_counter, static_cast<int64_t>(m));
    _num_rows_returned += m;

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
        RETURN_IF_ERROR(_process_build_block(block));
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

    ColumnRawPtrs raw_ptrs(1);
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_build_expr_ctxs[i]->execute(&block, &result_col_id));
        raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
    }

    Sizes sizes(1);
    I32KeyType key_getter(raw_ptrs, sizes, nullptr);
    // get_buffer_size_in_cells
    auto& build_data = assert_cast<const ColumnInt32*>(raw_ptrs[0])->get_data();
    Defer defer {[&]() {
        COUNTER_SET(_build_buckets_counter, (int64_t)_hash_table->get_buffer_size_in_cells());
    }};
    SCOPED_TIMER(_build_table_insert_timer);
    for (int k = 0; k < rows; ++k) {
        auto emplace_result = key_getter.emplace_key(*_hash_table, k, _arena);

        if (emplace_result.is_inserted()) {
            new (&emplace_result.get_mapped()) MapI32::mapped_type(build_data[k]);
        } else {
            /// The first element of the list is stored in the value of the hash table, the rest in the pool.
            emplace_result.get_mapped().inc();
        }
    }

    return Status::OK();
}

} // namespace doris::vectorized
