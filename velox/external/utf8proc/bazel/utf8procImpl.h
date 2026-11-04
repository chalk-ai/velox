/*
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

#include "utf8proc.h"

// Returns whether ch is a UTF-8 continuation byte.
#ifndef utf_cont
#define utf_cont(ch) (((ch)&0xc0) == 0x80)
#endif

#ifndef STRINGIZE
#define STRINGIZEx(x) #x
#define STRINGIZE(x) STRINGIZEx(x)
#endif

// Velox-specific additions to the upstream utf8proc API. The upstream
// library provides everything else; these helpers originate from DuckDB
// (http://www.zedwood.com/article/cpp-utf8-char-to-codepoint) and are kept
// here because upstream utf8proc has no equivalents.

/// Decodes the code point starting at 'u_input' and stores its byte length in
/// 'sz'. 'end' points to the first byte past the end of the string. Returns -1
/// on invalid or truncated input. Faster than utf8proc_iterate.
utf8proc_int32_t utf8proc_codepoint(const char* u_input, const char* end, int& sz);

/// Returns the size in bytes of the character pointed to by 'u_input'.
/// Assumes valid UTF-8 input; returns -1 for an invalid leading byte.
int utf8proc_char_length(const char* u_input);

/// Returns true if 'u_input' points to the first byte of a UTF-8 character.
/// A UTF-8 character may be 1 to 4 bytes long.
bool utf8proc_char_first_byte(const char* u_input);

/// Returns the size in bytes of the UTF-8 encoding of code point 'uc', or 0
/// for out-of-range values, matching the space written by
/// utf8proc_encode_char for invalid inputs.
int utf8proc_codepoint_length(utf8proc_int32_t uc);
