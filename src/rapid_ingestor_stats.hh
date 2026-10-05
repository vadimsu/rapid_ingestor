
#pragma once

namespace RapidIngestor {

struct RapidIngestorStats{
	RapidIngestorStats(): messagesParsed(0), bytesProcessed(0){}
	RapidIngestorStats(uint64_t parsed, uint64_t processed): messagesParsed(parsed), bytesProcessed(processed){}
	uint64_t messagesParsed;
	uint64_t bytesProcessed;
	RapidIngestorStats operator+(RapidIngestorStats other){
		return RapidIngestorStats {
			messagesParsed + other.messagesParsed,
			bytesProcessed + other.bytesProcessed };
	}
	RapidIngestorStats& operator+=(RapidIngestorStats other){
		messagesParsed += other.messagesParsed;
		bytesProcessed += other.bytesProcessed;
		return *this;
	}
};

}
