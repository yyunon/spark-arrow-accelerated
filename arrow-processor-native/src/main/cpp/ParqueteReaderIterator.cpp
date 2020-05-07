//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "ParqueteReaderIterator.h"

#include "my_assert.h"

#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>

ParqueteReaderIterator::ParqueteReaderIterator(const char *file_path) {
    ASSERT_OK(parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                               parquet::ParquetFileReader::OpenFile(file_path),
                                               &reader));

    ASSERT_OK(reader->GetRecordBatchReader({0}, &rb_reader));
}

bool ParqueteReaderIterator::hasNext() {
    if (next_batch != nullptr) {
        return true;
    }
    ASSERT_OK(rb_reader->ReadNext(&next_batch));
    if (nullptr != next_batch) {
        return true;
    }
    return false;
}

std::shared_ptr<arrow::RecordBatch> ParqueteReaderIterator::next() {
    if (next_batch != nullptr) {
        current_batch = next_batch;
        next_batch = nullptr;
    } else {
        ASSERT_OK(rb_reader->ReadNext(&current_batch));
    }
    return current_batch;
}