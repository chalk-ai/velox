#include "utf8procImpl.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#endif

extern "C" {

UTF8PROC_DLLEXPORT utf8proc_int32_t
utf8proc_codepoint(const char* u_input, const char* end, int& sz) {
  auto u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  if (u0 <= 127) {
    sz = 1;
    return u0;
  }
  if (end - u_input < 2) {
    return -1;
  }
  unsigned char u1 = u[1];
  if (u0 >= 192 && u0 <= 223) {
    sz = 2;
    return (u0 - 192) * 64 + (u1 - 128);
  }
  if (u[0] == 0xed && (u[1] & 0xa0) == 0xa0) {
    return -1; // code points, 0xd800 to 0xdfff
  }
  if (end - u_input < 3) {
    return -1;
  }
  unsigned char u2 = u[2];
  if (u0 >= 224 && u0 <= 239) {
    sz = 3;
    return (u0 - 224) * 4096 + (u1 - 128) * 64 + (u2 - 128);
  }
  if (end - u_input < 4) {
    return -1;
  }
  unsigned char u3 = u[3];
  if (u0 >= 240 && u0 <= 247) {
    sz = 4;
    return (u0 - 240) * 262144 + (u1 - 128) * 4096 + (u2 - 128) * 64 +
        (u3 - 128);
  }
  return -1;
}

UTF8PROC_DLLEXPORT int utf8proc_char_length(const char* u_input) {
  auto u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  if (u0 <= 127) {
    return 1;
  }

  if (u0 >= 192 && u0 <= 223) {
    return 2;
  }

  if (u0 >= 224 && u0 <= 239) {
    return 3;
  }

  if (u0 >= 240 && u0 <= 247) {
    return 4;
  }
  return -1;
}

UTF8PROC_DLLEXPORT bool utf8proc_char_first_byte(const char* u_input) {
  auto u = (const unsigned char*)u_input;
  unsigned char u0 = u[0];
  return u0 <= 127 || u0 >= 192;
}

UTF8PROC_DLLEXPORT int utf8proc_codepoint_length(utf8proc_int32_t uc) {
  if (uc < 0x00) {
    return 0;
  } else if (uc < 0x80) {
    return 1;
  } else if (uc < 0x800) {
    return 2;
  } else if (uc < 0x10000) {
    return 3;
  } else if (uc < 0x110000) {
    return 4;
  } else
    return 0;
}

} // extern "C"

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
