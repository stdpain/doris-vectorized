#pragma once
#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "vec/exec/join/vhash_table.hpp"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {
class VExprContext;

class HashJoinNode : public ::doris::ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // TODO hash table
    // TODO iterator
    // other join expr
    // TODO: make this thread not block
    Status hash_table_build(RuntimeState* state);

    using VExprContexts = std::vector<VExprContext*>;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;
    // other expr
    VExprContexts _other_join_conjunct_ctxs;

    std::vector<bool> _is_null_safe_eq_join;

    std::vector<int> _build_tuple_idx;

    DataTypes right_table_data_types;
    DataTypes left_table_data_types;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_hash_calc_timer;
    RuntimeProfile::Counter* _build_bucket_calc_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_spread_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _build_acquire_block_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_hash_calc_timer;
    RuntimeProfile::Counter* _probe_gather_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _probe_select_miss_timer;
    RuntimeProfile::Counter* _probe_select_zero_timer;
    RuntimeProfile::Counter* _probe_diff_timer;
    RuntimeProfile::Counter* _build_buckets_counter;

    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;
    RuntimeProfile::HighWaterMarkCounter* _probe_max_length;

    RuntimeProfile::Counter* _hash_tbl_load_factor_counter;

    bool _build_unique;
    size_t _build_tuple_size;

    VectorizedHashTable _hash_table;

    using Vec = std::vector<size_t>;
    using GroupIdV = Vec;
    using BucketV = Vec;
    using CheckV = Vec;
    using DifferV = Vec;
    using VLength = size_t;

    GroupIdV _group_id_vec;
    BucketV _bucket_vec;
    CheckV _to_check_vec;
    DifferV _differs_vec;

    Columns _build_columns;
    ColumnNumbers _build_column_numbers;
    ColumnNumbers _probe_column_numbers;

    using BlockList = std::vector<Block>;
    BlockList _block_list;
    int64_t _hash_table_rows;

    ColumnPtr _hash_column;

private:
    Status _process_build_block(Block& block);

    // use input expr as input
    // output:
    //  _bucket_vec
    //  _calc_hash
    Status _calc_hash(Block& block, VExprContexts& input_expr, ColumnNumbers& input_column,
                      int rows, RuntimeProfile::Counter* expr_timer,
                      RuntimeProfile::Counter* hash_calc_timer);

    Status _lookup_initial(Block& block, GroupIdV& groupIdV, CheckV& toCheckV, int n);

    void _check_column(DifferV& differV, CheckV& checkV, GroupIdV& groupV, MutableColumnPtr& value,
                       ColumnPtr& key, int m);

    int _select_miss(GroupIdV& groupV, CheckV& toCheckV, DifferV& differV, int m);

    int _select_notzero(GroupIdV& groupV, CheckV& toCheckV, int m);

    void _find_next(CheckV& toCheckV, Vec& next, GroupIdV& groupIdV, int m);

    // TODO: provide a iterator
    void _gather(GroupIdV& groupV, CheckV& toCheckV, const Block& left_block,
                 MutableBlock& output_mblock, int m);
    FunctionBasePtr _hash_func;
    FunctionBasePtr _mod_func;
};
} // namespace vectorized
} // namespace doris