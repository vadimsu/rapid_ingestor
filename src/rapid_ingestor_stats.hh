
#pragma once

namespace RapidIngestor {

struct RapidIngestorStats{
	RapidIngestorStats(): messagesParsed(0), bytesProcessed(0), rowsInserted(0){}
	RapidIngestorStats(uint64_t parsed, uint64_t processed, uint64_t inserted = 0): messagesParsed(parsed), bytesProcessed(processed), rowsInserted(inserted){}
	uint64_t messagesParsed;
	uint64_t bytesProcessed;
	uint64_t rowsInserted;
	RapidIngestorStats operator+(RapidIngestorStats other){
		return RapidIngestorStats {
			messagesParsed + other.messagesParsed,
			bytesProcessed + other.bytesProcessed,
			rowsInserted + other.rowsInserted };
	}
	RapidIngestorStats& operator+=(RapidIngestorStats other){
		messagesParsed += other.messagesParsed;
		bytesProcessed += other.bytesProcessed;
		rowsInserted += other.rowsInserted;
		return *this;
	}
};

}
