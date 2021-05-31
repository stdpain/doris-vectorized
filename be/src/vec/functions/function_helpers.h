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

#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/column_numbers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class IFunction;

/// Methods, that helps dispatching over real column types.

template <typename ... Type>
bool checkDataType(const IDataType* data_type) {
    return ((typeid_cast<const Type*>(data_type)) || ...);
}

template <typename Type>
const Type* checkAndGetDataType(const IDataType* data_type) {
    return typeid_cast<const Type*>(data_type);
}

template <typename Type>
const ColumnConst* checkAndGetColumnConst(const IColumn* column) {
    if (!column || !isColumnConst(*column)) return {};

    const ColumnConst* res = assert_cast<const ColumnConst*>(column);

    if (!checkColumn<Type>(&res->getDataColumn())) return {};

    return res;
}

template <typename Type>
const Type* checkAndGetColumnConstData(const IColumn* column) {
    const ColumnConst* res = checkAndGetColumnConst<Type>(column);

    if (!res) return {};

    return static_cast<const Type*>(&res->getDataColumn());
}

template <typename Type>
bool checkColumnConst(const IColumn* column) {
    return checkAndGetColumnConst<Type>(column);
}

/// Returns non-nullptr if column is ColumnConst with ColumnString or ColumnFixedString inside.
const ColumnConst* checkAndGetColumnConstStringOrFixedString(const IColumn* column);

/// Transform anything to Field.
template <typename T>
inline std::enable_if_t<!IsDecimalNumber<T>, Field> toField(const T& x) {
    return Field(NearestFieldType<T>(x));
}

template <typename T>
inline std::enable_if_t<IsDecimalNumber<T>, Field> toField(const T& x, UInt32 scale) {
    return Field(NearestFieldType<T>(x, scale));
}

Columns convertConstTupleToConstantElements(const ColumnConst& column);

/// Returns the copy of a given block in which each column specified in
/// the "arguments" parameter is replaced with its respective nested
/// column if it is nullable.
Block createBlockWithNestedColumns(const Block& block, const ColumnNumbers& args);

/// Similar function as above. Additionally transform the result type if needed.
Block createBlockWithNestedColumns(const Block& block, const ColumnNumbers& args, size_t result);

/// Checks argument type at specified index with predicate.
/// throws if there is no argument at specified index or if predicate returns false.
void validateArgumentType(const IFunction& func, const DataTypes& arguments, size_t argument_index,
                          bool (*validator_func)(const IDataType&),
                          const char* expected_type_description);

} // namespace doris::vectorized