// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <initializer_list>
#include <list>
#include <map>
#include <set>
#include <vector>

#include "vec/core/block_info.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/names_and_types.h"

namespace doris {
class Status;
namespace vectorized {

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitrary position, to change order of columns.
  */

class Context;

class Block {
private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = std::map<String, size_t>;

    Container data;
    IndexByName index_by_name;

public:
    BlockInfo info;

    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(const ColumnsWithTypeAndName& data_);
    Block(const PBlock& pblock);

    /// insert the column at the specified position
    void insert(size_t position, const ColumnWithTypeAndName& elem);
    void insert(size_t position, ColumnWithTypeAndName&& elem);
    /// insert the column to the end
    void insert(const ColumnWithTypeAndName& elem);
    void insert(ColumnWithTypeAndName&& elem);
    /// insert the column to the end, if there is no column with that name yet
    void insertUnique(const ColumnWithTypeAndName& elem);
    void insertUnique(ColumnWithTypeAndName&& elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the columns at the specified positions
    void erase(const std::set<size_t>& positions);
    /// remove the column with the specified name
    void erase(const String& name);
    // T was std::set<int>, std::vector<int>, std::list<int>
    template <class T>
    void erase_not_in(const T& container) {
        Container new_data;
        for(auto pos: container) {
            new_data.emplace_back(std::move(data[pos]));
        }
        std::swap(data, new_data);
    }

    /// References are invalidated after calling functions above.

    ColumnWithTypeAndName& getByPosition(size_t position) { return data[position]; }
    const ColumnWithTypeAndName& getByPosition(size_t position) const { return data[position]; }

    ColumnWithTypeAndName& safeGetByPosition(size_t position);
    const ColumnWithTypeAndName& safeGetByPosition(size_t position) const;

    ColumnWithTypeAndName& getByName(const std::string& name);
    const ColumnWithTypeAndName& getByName(const std::string& name) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    bool has(const std::string& name) const;

    size_t getPositionByName(const std::string& name) const;

    const ColumnsWithTypeAndName& getColumnsWithTypeAndName() const;
    NamesAndTypesList getNamesAndTypesList() const;
    Names getNames() const;
    DataTypes getDataTypes() const;

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    // Cut the rows in block, use in LIMIT operation
    void set_num_rows(size_t length);

    size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void checkNumberOfRows(bool allow_null_columns = false) const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    size_t allocatedBytes() const;

    operator bool() const { return !!columns(); }
    bool operator!() const { return !this->operator bool(); }

    /** Get a list of column names separated by commas. */
    std::string dumpNames() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
    std::string dumpStructure() const;

    /** Get the same block, but empty. */
    Block cloneEmpty() const;

    Columns getColumns() const;
    void setColumns(const Columns& columns);
    Block cloneWithColumns(const Columns& columns) const;
    Block cloneWithoutColumns() const;

    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutateColumns();

    /** Replace columns in a block */
    void setColumns(MutableColumns&& columns);
    Block cloneWithColumns(MutableColumns&& columns) const;

    /** Get a block with columns that have been rearranged in the order of their names. */
    Block sortColumns() const;

    void clear();
    void swap(Block& other) noexcept;
    void swap(Block&& other) noexcept;

    /** Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
    void updateHash(SipHash& hash) const;

    /** Get block data in string. */
    std::string dumpData(size_t row_limit = 100) const;

    static Status filter_block(Block* block, int filter_conlumn_id, int column_to_keep);
    // serialize block to PRowBatch
    void serialize(PBlock* pblock) const;

private:
    void eraseImpl(size_t position);
    void initializeIndexByName();
};

using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;
using BlocksPtrs = std::shared_ptr<std::vector<BlocksPtr>>;

/// Compare number of columns, data types, column types, column names, and values of constant columns.
// bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Throw exception when blocks are different.
// void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, const std::string & context_description);

/// Calculate difference in structure of blocks and write description into output strings. NOTE It doesn't compare values of constant columns.
// void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);

class MutableBlock {
private:
    MutableColumns _columns;
    DataTypes _data_types;

public:
    MutableBlock() = default;
    ~MutableBlock() = default;

    MutableBlock(MutableColumns&& columns, DataTypes&& data_types)
            : _columns(std::move(columns)), _data_types(std::move(data_types)) {}
    MutableBlock(Block* block)
            : _columns(block->mutateColumns()), _data_types(block->getDataTypes()) {}

    size_t rows() const;
    size_t columns() const { return _columns.size(); }

    bool empty() { return rows() == 0; }

    MutableColumns& mutable_columns() { return _columns; }

    DataTypes& data_types() { return _data_types; }

    void merge(Block&& block) {
        if (_columns.size() == 0 && _data_types.size() == 0) {
            _data_types = std::move(block.getDataTypes());
            _columns.resize(block.columns());
            for (size_t i = 0; i < block.columns(); ++i) {
                if (block.getByPosition(i).column) {
                    _columns[i] =
                            (*std::move(
                                     block.getByPosition(i).column->convertToFullColumnIfConst()))
                                    .mutate();
                } else {
                    _columns[i] = _data_types[i]->createColumn();
                }
            }
        } else {
            for (int i = 0; i < _columns.size(); ++i) {
                _columns[i]->insertRangeFrom(
                        *block.getByPosition(i).column->convertToFullColumnIfConst().get(), 0,
                        block.rows());
            }
        }
    }
    void merge(Block& block) {
        if (_columns.size() == 0 && _data_types.size() == 0) {
            _data_types = block.getDataTypes();
            _columns.resize(block.columns());
            for (size_t i = 0; i < block.columns(); ++i) {
                if (block.getByPosition(i).column) {
                    _columns[i] =
                            (*std::move(
                                     block.getByPosition(i).column->convertToFullColumnIfConst()))
                                    .mutate();
                } else {
                    _columns[i] = _data_types[i]->createColumn();
                }
            }
        } else {
            for (int i = 0; i < _columns.size(); ++i) {
                _columns[i]->insertRangeFrom(
                        *block.getByPosition(i).column->convertToFullColumnIfConst().get(), 0,
                        block.rows());
            }
        }
    }

    Block to_block();

    void add_row(const Block* block, int row);
    std::string dumpData(size_t row_limit = 100) const;

    void clear() {
        _columns.clear();
        _data_types.clear();
    }

    // TODO: use add_rows instead of this
    // add_rows(Block* block,PODArray<Int32>& group, int group_num);
};

} // namespace vectorized
} // namespace doris