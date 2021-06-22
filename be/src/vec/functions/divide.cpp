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

#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct DivideFloatingImpl {
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) / b;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* left, llvm::Value* right,
                                       bool) {
        if (left->getType()->is_integer_ty())
            LOG(FATAL) << "DivideFloatingImpl expected a floating-point type";
        }

        return b.CreateFDiv(left, right);
    }
#endif
};

struct NameDivide {
    static constexpr auto name = "divide";
};
using FunctionDivide = FunctionBinaryArithmetic<DivideFloatingImpl, NameDivide>;

void register_function_divide(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDivide>();
}

} // namespace doris::vectorized
