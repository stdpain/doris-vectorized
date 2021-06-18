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

#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

template <typename Transform>
class FunctionDateOrDateTimeToString : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeToString>(); }

    String get_name() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) override {
        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;
        const auto* sources = check_and_get_column<ColumnVector<Int128>>(source_col.get());
        auto col_res = ColumnString::create();
        if (sources) {
            TransformerToStringOneArgument<Transform>::vector(
                    sources->get_data(), col_res->get_chars(), col_res->get_offsets());
            block.getByPosition(result).column = std::move(col_res);
        } else {
            return Status::InternalError("Illegal column " +
                                                 block.getByPosition(arguments[0]).column->get_name() +
                                         " of first argument of function " + name);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
