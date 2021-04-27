#pragma once

#include "vec/common/typeid_cast.h"
#include "vec/common/assert_cast.h"
#include "vec/core/sort_description.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::vectorized
{

/** Cursor allows to compare rows in different blocks (and parts).
  * Cursor moves inside single block.
  * It is used in priority queue.
  */
struct SortCursorImpl
{
    ColumnRawPtrs all_columns;
    ColumnRawPtrs sort_columns;
    SortDescription desc;
    size_t sort_columns_size = 0;
    size_t pos = 0;
    size_t rows = 0;

    /** Determines order if comparing columns are equal.
      * Order is determined by number of cursor.
      *
      * Cursor number (always?) equals to number of merging part.
      * Therefore this field can be used to determine part number of current row (see ColumnGathererStream).
      */
    size_t order = 0;

    using NeedCollationFlags = std::vector<UInt8>;

    /** Should we use Collator to sort a column? */
    NeedCollationFlags need_collation;

    /** Is there at least one column with Collator. */
    bool has_collation = false;

    SortCursorImpl() = default;

    SortCursorImpl(const Block & block, const SortDescription & desc_, size_t order_ = 0)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size()) {
        reset(block);
    }

    SortCursorImpl(const Columns & columns, const SortDescription & desc_, size_t order_ = 0)
        : desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size()) {
        for (auto & column_desc : desc)
        {
            if (!column_desc.column_name.empty())
                throw Exception("SortDesctiption should contain column position if SortCursor was used without header.",
                        ErrorCodes::LOGICAL_ERROR);
        }
        reset(columns, {});
    }

    bool empty() const { return rows == 0; }

    /// Set the cursor to the beginning of the new block.
    void reset(const Block & block)
    {
        reset(block.getColumns(), block);
    }

    /// Set the cursor to the beginning of the new block.
    void reset(const Columns & columns, const Block & block)
    {
        all_columns.clear();
        sort_columns.clear();

        size_t num_columns = columns.size();

        for (size_t j = 0; j < num_columns; ++j)
            all_columns.push_back(columns[j].get());

        for (size_t j = 0, size = desc.size(); j < size; ++j)
        {
            auto & column_desc = desc[j];
            size_t column_number = !column_desc.column_name.empty()
                                   ? block.getPositionByName(column_desc.column_name)
                                   : column_desc.column_number;
            sort_columns.push_back(columns[column_number].get());

//            need_collation[j] = desc[j].collator != nullptr && typeid_cast<const ColumnString *>(sort_columns.back());    /// TODO Nullable(String)
//            has_collation |= need_collation[j];
        }

        pos = 0;
        rows = all_columns[0]->size();
    }

    bool isFirst() const { return pos == 0; }
    bool isLast() { return pos + 1 >= rows; }
    void next() { ++pos; }

    virtual bool has_next_block() { return false; }
    virtual Block* block_ptr() { return nullptr; }
};

using BlockSupplier = std::function<Status(Block **)>;

struct ReceiveQueueSortCursorImpl : public SortCursorImpl {
    ReceiveQueueSortCursorImpl(const BlockSupplier& block_supplier, const std::vector<VExprContext*>& ordering_expr, const std::vector<bool>& is_asc_order,
            const std::vector<bool>& nulls_first):
        SortCursorImpl(), _ordering_expr(ordering_expr), _block_supplier(block_supplier){
        sort_columns_size = ordering_expr.size();

        desc.resize(ordering_expr.size());
        for (int i = 0; i < desc.size(); i++) {
            desc[i].direction = is_asc_order[i] ? 1 : -1;
            desc[i].nulls_direction = nulls_first[i] ? 1 : -1;
        }
        has_next_block();
    }

    bool has_next_block() override {
        auto status = _block_supplier(&_block_ptr);
        if (status.ok() && _block_ptr != nullptr) {
            for (int i = 0; i < desc.size(); ++i) {
                _ordering_expr[i]->execute(_block_ptr, &desc[i].column_number);
            }
            SortCursorImpl::reset(*_block_ptr);
            return true;
        }
        _block_ptr = nullptr;
        return false;
    }

    Block* block_ptr() override {
        return _block_ptr;
    }

    size_t columns_num() const { return all_columns.size(); }

    Block create_empty_blocks() const {
        size_t num_columns = columns_num();
        MutableColumns columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            columns[i] = all_columns[i]->cloneEmpty();
        return _block_ptr->cloneWithColumns(std::move(columns));
    }

    const std::vector<VExprContext*>& _ordering_expr;
    Block* _block_ptr = nullptr;
    BlockSupplier _block_supplier{};
    bool _is_eof = false;
};

/// For easy copying.
struct SortCursor
{
    SortCursorImpl * impl;

    SortCursor(SortCursorImpl * impl_) : impl(impl_) {}
    SortCursorImpl * operator-> () { return impl; }
    const SortCursorImpl * operator-> () const { return impl; }

    /// The specified row of this cursor is greater than the specified row of another cursor.
    bool greaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
    {
        for (size_t i = 0; i < impl->sort_columns_size; ++i)
        {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);
            if (res > 0)
                return true;
            if (res < 0)
                return false;
        }
        return impl->order > rhs.impl->order;
    }

    /// Checks that all rows in the current block of this cursor are less than or equal to all the rows of the current block of another cursor.
    bool totallyLessOrEquals(const SortCursor & rhs) const
    {
        if (impl->rows == 0 || rhs.impl->rows == 0)
            return false;

        /// The last row of this cursor is no larger than the first row of the another cursor.
        return !greaterAt(rhs, impl->rows - 1, 0);
    }

    bool greater(const SortCursor & rhs) const
    {
        return greaterAt(rhs, impl->pos, rhs.impl->pos);
    }

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool operator< (const SortCursor & rhs) const
    {
        return greater(rhs);
    }
};


/// Separate comparator for locale-sensitive string comparisons
//struct SortCursorWithCollation
//{
//    SortCursorImpl * impl;
//
//    SortCursorWithCollation(SortCursorImpl * impl_) : impl(impl_) {}
//    SortCursorImpl * operator-> () { return impl; }
//    const SortCursorImpl * operator-> () const { return impl; }
//
//    bool greaterAt(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
//    {
//        for (size_t i = 0; i < impl->sort_columns_size; ++i)
//        {
//            int direction = impl->desc[i].direction;
//            int nulls_direction = impl->desc[i].nulls_direction;
//            int res;
//            if (impl->need_collation[i])
//            {
//                const ColumnString & column_string = assert_cast<const ColumnString &>(*impl->sort_columns[i]);
//                res = column_string.compareAtWithCollation(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), *impl->desc[i].collator);
//            }
//            else
//                res = impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), nulls_direction);
//
//            res *= direction;
//            if (res > 0)
//                return true;
//            if (res < 0)
//                return false;
//        }
//        return impl->order > rhs.impl->order;
//    }
//
//    bool totallyLessOrEquals(const SortCursorWithCollation & rhs) const
//    {
//        if (impl->rows == 0 || rhs.impl->rows == 0)
//            return false;
//
//        /// The last row of this cursor is no larger than the first row of the another cursor.
//        return !greaterAt(rhs, impl->rows - 1, 0);
//    }
//
//    bool greater(const SortCursorWithCollation & rhs) const
//    {
//        return greaterAt(rhs, impl->pos, rhs.impl->pos);
//    }
//
//    bool operator< (const SortCursorWithCollation & rhs) const
//    {
//        return greater(rhs);
//    }
//};

}