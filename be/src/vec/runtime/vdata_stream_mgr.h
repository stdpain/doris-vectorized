#pragma once
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

#include "common/global_types.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {
class RuntimeState;
class RowDescriptor;
class TUniqueId;
class RuntimeProfile;
class QueryStatisticsRecvr;
class PTransmitDataParams;

namespace vectorized {
class VDataStreamRecvr;

class VDataStreamMgr {
public:
    VDataStreamMgr();
    ~VDataStreamMgr();

    std::shared_ptr<VDataStreamRecvr> create_recvr(
            RuntimeState* state, const RowDescriptor& row_desc,
            const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
            int buffer_size, RuntimeProfile* profile, bool is_merging,
            std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    std::shared_ptr<VDataStreamRecvr> find_recvr(const TUniqueId& fragment_instance_id,
                                                 PlanNodeId node_id, bool acquire_lock = true);

    Status deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

    Status transmit_block(const PTransmitDataParams* request, ::google::protobuf::Closure** done);

    void cancel(const TUniqueId& fragment_instance_id);

private:
    std::mutex _lock;
    using StreamMap = std::unordered_multimap<uint32_t, std::shared_ptr<VDataStreamRecvr>>;
    StreamMap _receiver_map;

    struct ComparisonOp {
        bool operator()(const std::pair<doris::TUniqueId, PlanNodeId>& a,
                        const std::pair<doris::TUniqueId, PlanNodeId>& b) const {
            if (a.first.hi < b.first.hi) {
                return true;
            } else if (a.first.hi > b.first.hi) {
                return false;
            } else if (a.first.lo < b.first.lo) {
                return true;
            } else if (a.first.lo > b.first.lo) {
                return false;
            }
            return a.second < b.second;
        }
    };
    using FragmentStreamSet = std::set<std::pair<TUniqueId, PlanNodeId>, ComparisonOp>;
    FragmentStreamSet _fragment_stream_set;

    inline uint32_t get_hash_value(const TUniqueId& fragment_instance_id, PlanNodeId node_id);
};
} // namespace vectorized
} // namespace doris
