
#pragma once

namespace RapidIngestor {

struct RapidIngestorStats{
	RapidIngestorStats(): messagesParsed(0), bytesProcessed(0), rowsInserted(0), unparsedBufferSize(0), fatalErrors(0) {}
	RapidIngestorStats(uint64_t parsed, uint64_t processed, uint64_t inserted = 0, uint64_t unparsed = 0, uint64_t fatal = 0): messagesParsed(parsed), bytesProcessed(processed), rowsInserted(inserted), unparsedBufferSize(unparsed), fatalErrors(fatal) {}
	uint64_t messagesParsed;
	uint64_t bytesProcessed;
	uint64_t rowsInserted;
	uint64_t unparsedBufferSize;
	uint64_t fatalErrors;
	RapidIngestorStats operator+(RapidIngestorStats other){
		return RapidIngestorStats {
			messagesParsed + other.messagesParsed,
			bytesProcessed + other.bytesProcessed,
			rowsInserted + other.rowsInserted,
			unparsedBufferSize + other.unparsedBufferSize,
			fatalErrors + other.fatalErrors
		};
	}
	RapidIngestorStats& operator+=(RapidIngestorStats other){
		messagesParsed += other.messagesParsed;
		bytesProcessed += other.bytesProcessed;
		rowsInserted += other.rowsInserted;
		unparsedBufferSize += other.unparsedBufferSize;
		fatalErrors += other.fatalErrors;
		return *this;
	}
};

}
