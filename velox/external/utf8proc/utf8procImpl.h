
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

/*
 *  Copyright (c) 2014-2019 Steven G. Johnson, Jiahao Chen, Peter Colberg, Tony
 * Kelman, Scott P. Jones, and other contributors. Copyright (c) 2009 Public
 * Software Group e. V., Berlin, Germany
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"),
 *  to deal in the Software without restriction, including without limitation
 *  the rights to use, copy, modify, merge, publish, distribute, sublicense,
 *  and/or sell copies of the Software, and to permit persons to whom the
 *  Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

/*
 *  This library contains derived data from a modified version of the
 *  Unicode data files.
 *
 *  The original data files are available at
 *  http://www.unicode.org/Public/UNIDATA/
 *
 *  Please notice the copyright statement in the file "utf8proc_data.c".
 */

/*
 *  File name:    utf8proc.c
 *
 *  Description:
 *  Implementation of libutf8proc.
 */

#pragma once

#include "utf8proc.h"

#ifndef utf_cont
#define utf_cont(ch) (((ch)&0xc0) == 0x80)
#endif

#ifndef STRINGIZE
#define STRINGIZEx(x) #x
#define STRINGIZE(x) STRINGIZEx(x)
#endif


#ifdef __cplusplus
extern "C" {
#endif

// Custom Velox function declarations
UTF8PROC_DLLEXPORT utf8proc_int32_t utf8proc_codepoint(const char* u_input, const char* end, int& sz);
UTF8PROC_DLLEXPORT int utf8proc_char_length(const char* u_input);
UTF8PROC_DLLEXPORT bool utf8proc_char_first_byte(const char* u_input);
UTF8PROC_DLLEXPORT int utf8proc_codepoint_length(utf8proc_int32_t uc);

#ifdef __cplusplus
}
#endif
