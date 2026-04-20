/*
 *    WarnBridge – DAB Packet Mode Decoder Implementation
 *    Part of welle.io fork by Tobias / TogeriX-hub
 *
 *    Architecture derived from:
 *      - welle.io DabAudio (softbit pipeline, deinterleave, EEP deconvolution)
 *      - qt-dab data-processor (packet assembly logic, analysed clean-room)
 *
 *    GPL v2, same as welle.io.
 */

#include "dab-packet.h"
#include "eep-protection.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <sys/time.h>

// ---------------------------------------------------------------------------
// Fraunhofer NewsService Journaline(R) Decoder
// Copyright (c) 2003, 2001-2014 Fraunhofer IIS, Erlangen – GPL v2
// Used for NON-COMMERCIAL purposes only (WarnBridge is open-source/non-commercial)
// "Features NewsService Journaline(R) decoder technology by Fraunhofer IIS"
// ---------------------------------------------------------------------------
#include "newsobject.h"
#include "NML.h"
#include "dabdatagroupdecoder.h"

// Interleave map – identical to DabAudio
static const int16_t interleaveMap[] = {0,8,4,12,2,10,6,14,1,9,5,13,3,11,7,15};

// ---------------------------------------------------------------------------
// CRC-16/CCITT – used for DAB PACKET level CRC (ETSI EN 300 401 §5.3.2.1)
// Note: the DataGroup CRC is handled separately by the Fraunhofer decoder.
// ---------------------------------------------------------------------------
static uint16_t crc16_bytes(const uint8_t* data, size_t len)
{
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= static_cast<uint16_t>(data[i]) << 8;
        for (int b = 0; b < 8; b++) {
            crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : (crc << 1);
        }
    }
    return crc ^ 0xFFFF;
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------
DabPacket::DabPacket(
        int16_t            fragmentSize,
        int16_t            bitRate,
        ProtectionSettings protection,
        uint16_t           packetAddress,
        ProgrammeHandlerInterface& handler)
    : handler_(handler)
    , packetAddress_(packetAddress)
    , fragmentSize_(fragmentSize)
    , bitRate_(bitRate)
    , mscBuffer_(64 * 32768)
    , running_(true)
{
    // EEP protection only (Journaline subchannels use EEP per ETSI EN 300 401)
    const bool isEepA = (protection.eepProfile == EEPProtectionProfile::EEP_A);
    protectionHandler_ = std::make_unique<EEPProtection>(
            bitRate, isEepA, static_cast<int>(protection.eepLevel));

    // Output vector: bitRate * 24 bits → bitRate * 3 bytes
    outV_.resize(static_cast<size_t>(bitRate) * 24);

    // Interleaver buffers
    for (int i = 0; i < 16; i++) {
        interleaveData_[i].resize(fragmentSize);
    }

    // Fraunhofer DAB datagroup decoder
    fraunhoferDecoder_ = DAB_DATAGROUP_DECODER_createDec(&DabPacket::onDataGroup, this);

    thread_ = std::thread(&DabPacket::run, this);
}

DabPacket::~DabPacket()
{
    running_ = false;
    dataAvailable_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
    if (fraunhoferDecoder_) {
        DAB_DATAGROUP_DECODER_deleteDec(fraunhoferDecoder_);
        fraunhoferDecoder_ = nullptr;
    }
}

// ---------------------------------------------------------------------------
// process() – called by MscHandler from the OFDM thread with raw softbits
// ---------------------------------------------------------------------------
int32_t DabPacket::process(const softbit_t* v, int16_t cnt)
{
    while (mscBuffer_.GetRingBufferWriteAvailable() <= cnt) {
        if (!running_) return 0;
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    mscBuffer_.putDataIntoBuffer(v, cnt);
    dataAvailable_.notify_all();
    return cnt;
}

// ---------------------------------------------------------------------------
// run() – background thread: deinterleave → deconvolve → dedisperse → assemble
// Mirrors DabAudio::run() exactly for the first three steps.
// ---------------------------------------------------------------------------
void DabPacket::run()
{
    std::clog << "DabPacket: started packetAddr=0x" << std::hex << packetAddress_ << std::dec << "\n";

    std::vector<softbit_t> data(fragmentSize_);
    std::vector<softbit_t> tempX(fragmentSize_);

    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        while (running_ &&
               mscBuffer_.GetRingBufferReadAvailable() <= fragmentSize_) {
            dataAvailable_.wait(lock);
        }
        if (!running_) break;
        lock.unlock();

        mscBuffer_.getDataFromBuffer(data.data(), fragmentSize_);

        // --- Deinterleave (identical to DabAudio) ---
        for (int16_t i = 0; i < fragmentSize_; i++) {
            tempX[i] = interleaveData_[
                (interleaverIndex_ + interleaveMap[i & 017]) & 017][i];
            interleaveData_[interleaverIndex_][i] = data[i];
        }
        interleaverIndex_ = (interleaverIndex_ + 1) & 0x0F;

        if (countForInterleaver_ <= 15) {
            countForInterleaver_++;
            continue;
        }

        // --- EEP Viterbi deconvolution ---
        protectionHandler_->deconvolve(tempX.data(), fragmentSize_, outV_.data());

        // --- Energy dispersal ---
        energyDispersal_.dedisperse(outV_);

        // --- Packet assembly ---
        handleDecodedFrame(outV_);
    }
}

// ---------------------------------------------------------------------------
// handleDecodedFrame
//
// outV_ contains bitRate*24 decoded bits (uint8_t 0/1).
// A frame can contain multiple DAB packets back to back.
// Packet sizes: 24, 48, 72, 96 bytes → 192, 384, 576, 768 bits.
//
// DAB packet bit layout (ETSI EN 300 401 §5.3.2.1):
//   bits  0.. 1: packet_length  (00=24B 01=48B 10=72B 11=96B)
//   bits  2.. 3: continuity_index
//   bits  4.. 5: first_last     (10=first 00=intermediate 01=last 11=single)
//   bits  6..15: packet_address (10 bits)
//   bit  16:     command_flag
//   bits 17..23: useful_data_length (7 bits)
//   bits 24..N*8-17: useful_data (useful_data_length bytes)
//   last 16 bits: CRC-16
// ---------------------------------------------------------------------------
void DabPacket::handleDecodedFrame(const std::vector<uint8_t>& bits)
{
    static const size_t pkt_sizes_bits[4] = { 192, 384, 576, 768 };
    size_t offset = 0;

    while (offset + 16 < bits.size()) {
        uint32_t pkt_len_code = (bits[offset] << 1) | bits[offset + 1];
        size_t pkt_bits = pkt_sizes_bits[pkt_len_code & 3];

        if (offset + pkt_bits > bits.size()) break;

        handlePacket(bits.data() + offset, pkt_bits);
        offset += pkt_bits;
    }
}

// ---------------------------------------------------------------------------
// handlePacket
//
// Processes one DAB packet (in bit form, 0/1 uint8_t).
// Assembles MSC data groups from first/intermediate/last/single packets.
// On completion calls handler_.onJournalineData().
//
// This logic mirrors qt-dab data-processor::handlePacket() (clean-room).
// ---------------------------------------------------------------------------
void DabPacket::handlePacket(const uint8_t* bits, size_t num_bits)
{
    // Minimum sanity check
    if (num_bits < 24 * 8) return;

    // Convert bit-vector to packed bytes for CRC check
    size_t num_bytes = num_bits / 8;
    std::vector<uint8_t> packed(num_bytes);
    for (size_t i = 0; i < num_bytes; i++) {
        uint8_t b = 0;
        for (int j = 0; j < 8; j++) {
            b = (b << 1) | (bits[i * 8 + j] & 1);
        }
        packed[i] = b;
    }

    // CRC check: CRC-16 over all bytes except last 2, result must equal last 2
    if (num_bytes < 3) return;
    uint16_t crc_calc = crc16_bytes(packed.data(), num_bytes - 2);
    uint16_t crc_pkt  = (static_cast<uint16_t>(packed[num_bytes - 2]) << 8)
                       | packed[num_bytes - 1];
    if (crc_calc != crc_pkt) {
        static int crcErrors = 0;
        if (++crcErrors == 1)
            std::clog << "DabPacket: first CRC error (signal quality)\n";
        return;
    }

    // Parse header from bits
    uint8_t cntIdx    = (bits[2] << 1) | bits[3];
    uint8_t firstLast = (bits[4] << 1) | bits[5];
    uint16_t paddr    = 0;
    for (int i = 0; i < 10; i++) paddr = (paddr << 1) | bits[6 + i];
    uint8_t udlen = 0;
    for (int i = 0; i < 7; i++) udlen = (udlen << 1) | bits[17 + i];

    // Address filter
    if (paddr != packetAddress_) return;

    // Skip if continuity index unchanged and not first packet
    if (cntIdx == static_cast<uint8_t>(lastCntIdx_) && firstLast != 2) return;

    // Continuity check – reset assembler on gap
    if (lastCntIdx_ >= 0 &&
        cntIdx != static_cast<uint8_t>((lastCntIdx_ + 1) % 4)) {
        assembling_ = false;
    }
    lastCntIdx_ = cntIdx;

    // Useful data starts at bit 24, length = udlen bytes = udlen*8 bits
    size_t data_offset_bits = 24;
    size_t data_bits = static_cast<size_t>(udlen) * 8;

    if (data_offset_bits + data_bits + 16 > num_bits) {
        // udlen exceeds packet
        assembling_ = false;
        return;
    }

    switch (firstLast) {
        case 2: { // First packet of data group
            series_.resize(data_bits);
            for (size_t i = 0; i < data_bits; i++)
                series_[i] = bits[data_offset_bits + i];
            assembling_ = true;
            break;
        }
        case 0: { // Intermediate packet
            if (!assembling_) return;
            size_t cur = series_.size();
            series_.resize(cur + data_bits);
            for (size_t i = 0; i < data_bits; i++)
                series_[cur + i] = bits[data_offset_bits + i];
            break;
        }
        case 1: { // Last packet
            if (!assembling_) return;
            size_t cur = series_.size();
            series_.resize(cur + data_bits);
            for (size_t i = 0; i < data_bits; i++)
                series_[cur + i] = bits[data_offset_bits + i];

            // Complete data group assembled – parse Journaline
            dispatchDataGroup(series_);
            series_.resize(0);
            assembling_ = false;
            break;
        }
        case 3: { // Single packet (complete data group)
            series_.resize(data_bits);
            for (size_t i = 0; i < data_bits; i++)
                series_[i] = bits[data_offset_bits + i];
            dispatchDataGroup(series_);
            series_.resize(0);
            assembling_ = false;
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// dispatchDataGroup
//
// series_ contains the complete MSC data group as a bit-vector (0/1 uint8_t).
// Converts to bytes, then feeds into the Fraunhofer DAB datagroup decoder
// which handles CRC, header parsing, and passes valid Journaline data groups
// to the NML parser.
//
// The Fraunhofer decoder expects:
//   - complete MSC data group (header + data field + optional CRC)
//   - data_group_type=0 (general data, Journaline uses this)
//   - crc_flag=1 (Journaline data groups always have CRC)
//   - segment_flag=0 (Journaline is never segmented)
// ---------------------------------------------------------------------------
void DabPacket::dispatchDataGroup(const std::vector<uint8_t>& bits)
{
    if (bits.size() < 16) return;

    // Convert bit-vector to byte-vector
    size_t num_bytes = bits.size() / 8;
    std::vector<uint8_t> bytes(num_bytes);
    for (size_t i = 0; i < num_bytes; i++) {
        uint8_t b = 0;
        for (int j = 0; j < 8; j++) b = (b << 1) | (bits[i * 8 + j] & 1);
        bytes[i] = b;
    }

    // DEBUG: erste 8 Bytes loggen
    std::clog << "DabPacket: dispatchDataGroup " << num_bytes << "B: ";
    for (size_t i = 0; i < std::min(num_bytes, (size_t)8); i++)
        std::clog << std::hex << std::setw(2) << std::setfill('0') << (int)bytes[i] << " ";
    std::clog << std::dec << "\n";

    // Feed into Fraunhofer DAB datagroup decoder.
    // It checks CRC, validates header, and calls our callback if valid.
    DAB_DATAGROUP_DECODER_putData(fraunhoferDecoder_, bytes.size(), bytes.data());
}

// ---------------------------------------------------------------------------
// Static callback – called by Fraunhofer decoder for each valid data group.
// buf/len = the MSC data field (after header, CRC stripped).
// This is the raw NML object: bytes 0-1 = object_id, byte 2 = type/flags,
// byte 3+ = (possibly compressed) content.
// ---------------------------------------------------------------------------
void DabPacket::onDataGroup(
        const DAB_DATAGROUP_DECODER_msc_datagroup_header_t* /*header*/,
        unsigned long len,
        const unsigned char* buf,
        void* arg)
{
    DabPacket* self = static_cast<DabPacket*>(arg);

    // Wrap in a timeval for NewsObject (only used internally, not exposed)
    struct timeval tv = {};
    gettimeofday(&tv, nullptr);

    // NewsObject validates and stores the raw NML bytes
    NewsObject newsObj(len, buf, &tv);

    // Copy NML for the factory
    NML::RawNewsObject_t rno;
    unsigned long nml_len = 0;
    newsObj.copyNml(&nml_len, rno.nml);
    rno.nml_len = static_cast<unsigned short>(nml_len);
    rno.extended_header_len = 0;

    // Parse NML using Fraunhofer factory (handles compression, 0x1a/0x1b
    // data sections, title/body/menu structure correctly)
    RemoveNMLEscapeSequences escHandler;
    NMLFactory factory;
    std::shared_ptr<NML> nml = factory.CreateNML(rno, &escHandler);

    if (!nml) {
        if (nml_len >= 2) {
            uint16_t oid = (static_cast<uint16_t>(buf[0]) << 8) | buf[1];
            uint8_t  raw_type = (nml_len >= 3) ? (buf[2] >> 5) : 0;
            self->handler_.onJournalineData(oid, raw_type, "JML-error", "");
        }
        return;
    }

    // isValid() is false for TITLE objects and some PLAIN objects in the
    // Fraunhofer decoder – but the title is correctly parsed regardless.
    // We accept any object that has a non-empty title and a known type.
    if (nml->GetObjectType() == NML::INVALID || nml->GetTitle().empty()) {
        if (nml_len >= 2) {
            uint16_t oid = (static_cast<uint16_t>(buf[0]) << 8) | buf[1];
            uint8_t  raw_type = (nml_len >= 3) ? (buf[2] >> 5) : 0;
            self->handler_.onJournalineData(oid, raw_type, "JML-error", "");
        }
        return;
    }

    uint16_t object_id = nml->GetObjectId();
    uint8_t  obj_type  = static_cast<uint8_t>(nml->GetObjectType());
    std::string title  = nml->GetTitle();
    std::string body;

    // For PLAIN text objects the body is in the first item
    if (nml->GetObjectType() == NML::PLAIN && nml->GetNrOfItems() > 0) {
        body = nml->GetItemText(0);
    }
    // For MENU objects concatenate item texts as body summary
    else if (nml->GetObjectType() == NML::MENU) {
        for (unsigned int i = 0; i < nml->GetNrOfItems(); i++) {
            if (i > 0) body += " | ";
            body += nml->GetItemText(i);
        }
    }

    self->handler_.onJournalineData(object_id, obj_type, title, body);
}