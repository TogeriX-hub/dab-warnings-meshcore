/*
 *    WarnBridge – DAB Packet Mode Decoder
 *    Part of welle.io fork by Tobias / TogeriX-hub
 *
 *    Decodes a DAB packet-mode subchannel (e.g. Journaline, DSCTy=0x44a).
 *    Pipeline: softbits → deinterleave → EEP Viterbi → energy dispersal →
 *    packet assembly → Fraunhofer DAB datagroup decoder → NML parser →
 *    ProgrammeHandlerInterface::onJournalineData()
 *
 *    Uses the Fraunhofer NewsService Journaline(R) Decoder for NML parsing.
 *    "Features NewsService Journaline(R) decoder technology by Fraunhofer IIS"
 *
 *    GPL v2, same as welle.io.
 */
#pragma once

#include <cstdint>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <string>
#include "dab-constants.h"
#include "radio-controller.h"
#include "eep-protection.h"
#include "energy_dispersal.h"
#include "ringbuffer.h"
#include "dabdatagroupdecoder.h"

// Forward declaration
class Protection;

class DabPacket {
public:
    DabPacket(
            int16_t              fragmentSize,
            int16_t              bitRate,
            ProtectionSettings   protection,
            uint16_t             packetAddress,
            ProgrammeHandlerInterface& handler);

    ~DabPacket();

    // Called by MscHandler with raw softbits from the CIF
    int32_t process(const softbit_t* v, int16_t cnt);

private:
    void run();
    void handleDecodedFrame(const std::vector<uint8_t>& bits);
    void handlePacket(const uint8_t* bits, size_t num_bits);
    void dispatchDataGroup(const std::vector<uint8_t>& bits);

    // Fraunhofer datagroup decoder callback (static → passed as C function ptr)
    static void onDataGroup(
            const DAB_DATAGROUP_DECODER_msc_datagroup_header_t* header,
            unsigned long len,
            const unsigned char* buf,
            void* arg);

    ProgrammeHandlerInterface& handler_;
    uint16_t                   packetAddress_;
    int16_t                    fragmentSize_;
    int16_t                    bitRate_;

    std::unique_ptr<EEPProtection> protectionHandler_;
    EnergyDispersal            energyDispersal_;

    RingBuffer<softbit_t>      mscBuffer_;
    std::mutex                 mutex_;
    std::condition_variable    dataAvailable_;
    std::atomic<bool>          running_;
    std::thread                thread_;

    // Interleaver state
    std::vector<softbit_t>     interleaveData_[16];
    int16_t                    interleaverIndex_   = 0;
    int16_t                    countForInterleaver_= 0;

    // Decoded output buffer (bitRate * 24 bits)
    std::vector<uint8_t>       outV_;

    // Packet assembly state
    bool                       assembling_    = false;
    int16_t                    lastCntIdx_    = -1;
    std::vector<uint8_t>       series_;

    // Fraunhofer DAB datagroup decoder instance
    DAB_DATAGROUP_DECODER_t    fraunhoferDecoder_ = nullptr;
};
