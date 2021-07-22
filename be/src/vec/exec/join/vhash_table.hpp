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

        // increase counter to reserve space
        ++counter;
        for (int i = 0; i < values.size(); ++i) {
            values[i]->insert_default();
        }
    }

    // before call insert reserve size must be called
    template <class GroupIdV, class BucketV>
    void insert(GroupIdV& groupIdV, BucketV& bucketV, int n) {
        for (int i = 0; i < n; i++) {
            groupIdV[i] = counter++;
            next[groupIdV[i]] = first[bucketV[i]];
            _bucket_filled_num += (first[bucketV[i]] == 0);
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

    size_t reserve_size() { return next.size(); }

    void reserve_size(size_t size) {
        if (size > reserve_size()) {
            size_t sz = round_up_to_power_of_two(size);
            _reserve_size(sz);
        }
    }

    void reserve_bucket() {
        if (_bucket_filled_num >= _bucket_till_resize) {
            size_t sz = bucket_size() * 2;
            resize_bucket(sz);
        }
    }

    void resize_bucket(size_t num_buckets) {
        DCHECK_EQ((num_buckets & (num_buckets - 1)), 0);
        int64_t old_num_buckets = bucket_size();

        first.resize(num_buckets);

        auto& hash_column = values[0];
        auto& hash_val_data = assert_cast<const ColumnUInt64*>(hash_column.get())->get_data();
        for (size_t i = 0; i < old_num_buckets; ++i) {
            // hash table
            auto group_id = first[i];
            size_t* p_last = &first[i];
            while (group_id) {
                uint64_t hash = hash_val_data[group_id];
                int32_t target_bucket = hash & (num_buckets - 1);
                size_t next_group_id = next[group_id];
                if (target_bucket != i) {
                    next[group_id] = first[target_bucket];
                    _bucket_filled_num += (first[target_bucket] == 0);
                    first[target_bucket] = group_id;

                    *p_last = next_group_id;
                    group_id = next_group_id;
                } else {
                    p_last = &next[group_id];
                    group_id = next_group_id;
                }
            }
            _bucket_filled_num -= (first[i] == 0);
        }
        _bucket_till_resize = num_buckets * 0.75;
    }

    const DataTypes& data_types() { return _data_types; }

    size_t counter;
    using Vec = std::vector<size_t>;
    Vec first;
    Vec next;
    MutableColumns values;
    DataTypes _data_types;

    size_t _bucket_filled_num = 0;
    size_t _bucket_till_resize = 0;

private:
    void _reserve_size(size_t sz) {
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
