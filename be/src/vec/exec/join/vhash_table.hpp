#pragma once
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"

namespace doris::vectorized {
// now don't support StringValue
struct VectorizedHashTable {
    VectorizedHashTable() : counter(0) {}
    ~VectorizedHashTable() = default;

    template <class GroupIdV, class BucketV>
    void insert(GroupIdV& groupIdV, BucketV& bucketV, int n) {
        reserve_size(counter + n);
        for (int i = 0; i < n; i++) {
            groupIdV[i] = counter++;
            next[groupIdV[i]] = first[bucketV[i]];
            first[bucketV[i]] = groupIdV[i];
        }
    }

    template <class GroupIdV>
    void spread(GroupIdV& groupIdV, MutableColumns& inputValues, int n) {
        for (int i = 0; i < values.size(); ++i) {
            _spread(groupIdV, inputValues[i], i, n);
        }
    }

    template <class GroupIdV>
    void _spread(GroupIdV& groupIdV, MutableColumnPtr& value_column, int col, int n) {
        values[col]->insert_range_from(*value_column, 0, n);
    }

    size_t bucket_size() { return first.size(); }

    void reserve_size(size_t size) {
        if (size > bucket_size()) {
            size_t sz = round_up_to_power_of_two(size);
            _reserve_size(sz);
        }
    }

    size_t counter;
    using Vec = std::vector<size_t>;
    Vec first;
    Vec next;
    MutableColumns values;
    DataTypes data_types;

private:
    void _reserve_size(size_t sz) {
        first.resize(sz);
        next.resize(sz);
        size_t column_size = data_types.size();
        for (size_t i = 0; i < column_size; ++i) {
            WhichDataType which(data_types[i]);
            if (which.is_int32()) {
                assert_cast<ColumnInt32*>(values[i].get())->get_data().resize(sz);
            } else {
                LOG(FATAL) << "not support datatype:" << int(which.idx);
            }
        }
    }

    static inline size_t round_up_to_power_of_two(size_t v) {
        --v;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v |= v >> 32;
        ++v;
        return v;
    }
};
} // namespace doris::vectorized
