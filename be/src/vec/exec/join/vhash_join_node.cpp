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
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.hash_join_node.join_op),
          _hash_table_rows(0) {}

HashJoinNode::~HashJoinNode() {}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;

    for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
        VExprContext* ctx = nullptr;
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
    bool has_eq_for_null = std::find(_is_null_safe_eq_join.begin(), _is_null_safe_eq_join.end(),
                                     true) != _is_null_safe_eq_join.end();
    if (has_eq_for_null) {
        return Status::NotSupported("Not Implemented VHashJoin Node join condition: eq_for_null");
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
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);

    RETURN_IF_ERROR(
            VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc(), expr_mem_tracker()));
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    RETURN_IF_ERROR(
            VExpr::prepare(_other_join_conjunct_ctxs, state, _row_descriptor, expr_mem_tracker()));

    // right table data types
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());
    // Hash Table Init

    _hash_table_variants.emplace<SerializedHashTableContext>();
    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented HashJoin Node::get_next scalar");
}

Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);
    size_t probe_rows = _probe_block.rows();
    if (probe_rows == 0 || _probe_index == probe_rows) {
        _probe_index = 0;
        _probe_block.clear();
        RETURN_IF_ERROR(child(0)->get_next(state, &_probe_block, eos));

        probe_rows = _probe_block.rows();
        if (probe_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
        _probe_columns.resize(probe_expr_ctxs_sz);

        for (int i = 0; i < probe_expr_ctxs_sz; ++i) {
            int result_id = -1;
            _probe_expr_ctxs[i]->execute(&_probe_block, &result_id);
            DCHECK_GE(result_id, 0);
            _probe_columns[i] = _probe_block.get_by_position(result_id).column.get();
        }
    }

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    using KeyGetter = typename HashTableCtxType::State;
                    using Mapped = typename HashTableCtxType::Mapped;

                    Sizes sizes(1);
                    KeyGetter key_getter(_probe_columns, sizes, nullptr);

                    MutableBlock mutable_block(
                            VectorizedUtils::create_empty_columnswithtypename(row_desc()));
                    output_block->clear();

                    IColumn::Offsets offset_data;
                    auto& mcol = mutable_block.mutable_columns();
                    offset_data.assign(probe_rows, (uint32_t)0);

                    int right_col_idx = _left_table_data_types.size();
                    int current_offset = 0;

                    for (; _probe_index < probe_rows; ++_probe_index) {
                        auto find_result =
                                key_getter.find_key(arg.hash_table, _probe_index, _arena);
                        if (find_result.is_found()) {
                            auto& mapped = find_result.get_mapped();

                            for (auto it = mapped.begin(); it.ok(); ++it) {
                                for (size_t j = 0, size = _right_table_data_types.size(); j < size;
                                     ++j) {
                                    auto& column = *it->block->get_by_position(j).column;
                                    mcol[j + right_col_idx]->insert_from(column, it->row_num);
                                }
                                ++current_offset;
                            }
                        }

                        offset_data[_probe_index] = current_offset;
                        if (current_offset >= state->batch_size()) {
                            break;
                        }
                    }

                    for (int i = _probe_index; i < probe_rows; ++i) {
                        offset_data[i] = current_offset;
                    }

                    output_block->swap(mutable_block.to_block());
                    for (int i = 0; i < right_col_idx; ++i) {
                        auto& column = _probe_block.get_by_position(i).column;
                        output_block->get_by_position(i).column = column->replicate(offset_data);
                    }

                    int64_t m = output_block->rows();
                    COUNTER_UPDATE(_rows_returned_counter, m);
                    _num_rows_returned += m;
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

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

    RETURN_IF_ERROR(_hash_table_build(state));
    RETURN_IF_ERROR(child(0)->open(state));

    return Status::OK();
}

Status HashJoinNode::_hash_table_build(RuntimeState* state) {
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
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }
    auto& acquired_block = _acquire_list.acquire(std::move(block));
    ColumnRawPtrs raw_ptrs(1);
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        RETURN_IF_ERROR(_build_expr_ctxs[i]->execute(&acquired_block, &result_col_id));
        raw_ptrs[i] = acquired_block.get_by_position(result_col_id).column.get();
    }

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    using KeyGetter = typename HashTableCtxType::State;
                    using Mapped = typename HashTableCtxType::Mapped;

                    Sizes sizes(1);
                    KeyGetter key_getter(raw_ptrs, sizes, nullptr);
                    // get_buffer_size_in_cells
                    Defer defer {[&]() {
                        int64_t bucket_size = arg.hash_table.get_buffer_size_in_cells();
                        COUNTER_SET(_build_buckets_counter, (int64_t)bucket_size);
                    }};
                    SCOPED_TIMER(_build_table_insert_timer);
                    for (size_t k = 0; k < rows; ++k) {
                        auto emplace_result = key_getter.emplace_key(arg.hash_table, k, _arena);
                        if (emplace_result.is_inserted()) {
                            new (&emplace_result.get_mapped()) Mapped({&acquired_block, k});
                        } else {
                            /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                            emplace_result.get_mapped().insert({&acquired_block, k}, _arena);
                        }
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return Status::OK();
}

} // namespace doris::vectorized
