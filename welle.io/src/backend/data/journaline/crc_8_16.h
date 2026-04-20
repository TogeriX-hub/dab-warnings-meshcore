/*
 * crc_8_16.h – CRC-16 implementation for Fraunhofer DAB decoder
 * Part of the NewsService Journaline(R) Decoder
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/* CRC-16/CCITT check: returns 1 if CRC matches, 0 otherwise */
static inline int CRC_Check_16(const unsigned char* buf, unsigned long len, unsigned short crc_field)
{
    unsigned short crc = 0xFFFF;
    for (unsigned long i = 0; i < len; i++) {
        crc ^= (unsigned short)buf[i] << 8;
        for (int b = 0; b < 8; b++) {
            crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : (crc << 1);
        }
    }
    crc ^= 0xFFFF;  /* DAB inverted CRC */
    return (crc == crc_field) ? 1 : 0;
}

#ifdef __cplusplus
}
#endif
