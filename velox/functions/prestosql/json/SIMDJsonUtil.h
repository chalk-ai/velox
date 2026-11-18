/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/functions/prestosql/json/SIMDJsonWrapper.h"

#include <folly/Preprocessor.h>
#include <folly/synchronization/CallOnce.h>

#include <array>
#include <exception>

#define SIMDJSON_ASSIGN_OR_RAISE_IMPL(_resultName, _lhs, _rexpr) \
  auto&& _resultName = (_rexpr);                                 \
  if (_resultName.error() != ::simdjson::SUCCESS) {              \
    return _resultName.error();                                  \
  }                                                              \
  _lhs = std::move(_resultName).value_unsafe();

/// Helper to extract simdjson_result<T>.  Return immediately if the result is
/// an error.  Examples:
///
///   std::string s;
///   SIMDJSON_ASSIGN_OR_RAISE(s, simdjson::to_json_string(v));
///
///   SIMDJSON_ASSIGN_OR_RAISE(auto s, simdjson::to_json_string(v));
#define SIMDJSON_ASSIGN_OR_RAISE(_lhs, _rexpr) \
  SIMDJSON_ASSIGN_OR_RAISE_IMPL(               \
      FB_ANONYMOUS_VARIABLE(_simdjsonResult), _lhs, _rexpr)

namespace facebook::velox {

/// Initialize the exceptions array with all possible simdjson error codes.
void simdjsonErrorsToExceptions(
    std::exception_ptr exceptions[simdjson::NUM_ERROR_CODES]);

/// Per-error-code cache of VeloxExceptions for simdjson errors, built on first
/// use.
///
/// Each VeloxException captures the current expression context, including
/// the full text of the top-level expression. Building one exception per
/// error code up front (simdjsonErrorsToExceptions) therefore stores
/// NUM_ERROR_CODES copies of that text in every function instance, even when
/// no error ever happens. Large expressions that are kept alive in an
/// ExprSetPool make this tens of GB. Building only the codes that actually
/// occur keeps the cost proportional to real errors.
///
/// get() must be called during expression evaluation so the exception gets
/// the right context, exactly like the eager initialization it replaces.
class SimdjsonErrors {
 public:
  const std::exception_ptr& get(simdjson::error_code code) const;

 private:
  mutable std::array<folly::once_flag, simdjson::NUM_ERROR_CODES> once_;
  mutable std::array<std::exception_ptr, simdjson::NUM_ERROR_CODES> errors_;
};

/// Parse the input json string using a thread local on demand parser.
simdjson::simdjson_result<simdjson::ondemand::document> simdjsonParse(
    const simdjson::padded_string_view& json);

/// Returns a simdjson::padded_string_view object from the input
/// std::string_view object to avoid creating a new copy of the
/// std::string_view. NOTE: The user MUST ensure that the input 'json' already
/// has simdjson::SIMDJSON_PADDING at the end.
simdjson::padded_string_view reusePaddedStringView(
    const std::string_view& json);

/// Parse the input json string using a thread local on demand parser. Allow
/// incomplete json input.
simdjson::simdjson_result<simdjson::ondemand::document> simdjsonParseIncomplete(
    const simdjson::padded_string_view& json);

} // namespace facebook::velox
