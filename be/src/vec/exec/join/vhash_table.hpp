#pragma once
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
// now don't support StringValue
struct VectorizedHashTable {
    VectorizedHashTable() : counter(0) {}
    ~VectorizedHashTable() = default;

    void init_values(DataTypes&& data_types) {
        values.reserve(data_types.size());
        for (const auto& type : data_types) {
            values.emplace_back(type->create_column());
        }
        _data_types = std::move(data_types);
    }

    // before call insert reserve size must be called
    template <class GroupIdV, class BucketV>
    void insert(GroupIdV& groupIdV, BucketV& bucketV, int n) {
        for (int i = 0; i < n; i++) {
            groupIdV[i] = counter++;
            next[groupIdV[i]] = first[bucketV[i]];
            first[bucketV[i]] = groupIdV[i];
        }
    }

    template <class GroupIdV>
    void spread(GroupIdV& groupIdV, Columns& inputValues, int n) {
        for (int i = 0; i < values.size(); ++i) {
            _spread(groupIdV, inputValues[i], i, n);
        }
    }

    template <class GroupIdV>
    void _spread(GroupIdV& groupIdV, ColumnPtr& value_column, int col, int n) {
        values[col]->insert_range_from(*value_column, 0, n);
    }

    size_t bucket_size() { return first.size(); }

    void reserve_size(size_t size) {
        if (size > bucket_size()) {
            size_t sz = round_up_to_power_of_two(size);
            _reserve_size(sz);
        }
    }

    const DataTypes& data_types() { return _data_types; }

    size_t counter;
    using Vec = std::vector<size_t>;
    Vec first;
    Vec next;
    MutableColumns values;
    DataTypes _data_types;

private:
    void _reserve_size(size_t sz) {
        first.resize(sz);
        next.resize(sz);
        for (int i = 0; i < values.size(); ++i) {
            values[i]->reserve(sz);
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
