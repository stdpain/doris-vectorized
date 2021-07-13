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

    // probe expr
    std::vector<VExprContext*> _probe_expr_ctxs;
    // build expr
    std::vector<VExprContext*> _build_expr_ctxs;
    // other expr
    std::vector<VExprContext*> _other_join_conjunct_ctxs;

    std::vector<bool> _is_null_safe_eq_join;

    std::vector<int> _build_tuple_idx;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _build_buckets_counter;

    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;

    RuntimeProfile::Counter* _hash_tbl_load_factor_counter;

    bool _build_unique;
    size_t _build_tuple_size;

    VectorizedHashTable _hash_table;

    using GroupIdV = std::vector<size_t>;
    using BucketV = std::vector<size_t>;

    GroupIdV _group_id_vec;
    BucketV _bucket_vec;

private:
    Status process_build_block(Block& block);
    FunctionBasePtr _hash_func;
    FunctionBasePtr _mod_func;
};
} // namespace vectorized
} // namespace doris