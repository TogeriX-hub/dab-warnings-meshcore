/*
 * dabdgdec_impl.h – internal header for dabdgdec_impl.c
 * Part of the Fraunhofer NewsService Journaline(R) Decoder
 */
#pragma once
#include "dabdatagroupdecoder.h"

#define DAB_DGDEC_MAGIC_ID 0xDABD600D

typedef struct {
    unsigned int magicId;
    DAB_DATAGROUP_DECODER_data* cb;
    void* arg;
} DAB_DGDEC_IMPL_t;

#ifdef __cplusplus
extern "C" {
#endif

int DAB_DGDEC_IMPL_checkCrc(const unsigned char* buf, unsigned long len, unsigned short crc_field);
int DAB_DGDEC_IMPL_extractMscDatagroupHeader(unsigned long len, const unsigned char* buf,
    DAB_DATAGROUP_DECODER_msc_datagroup_header_t* header);
void DAB_DGDEC_IMPL_showMscDatagroupHeader(DAB_DATAGROUP_DECODER_msc_datagroup_header_t* header);

#ifdef __cplusplus
}
#endif
