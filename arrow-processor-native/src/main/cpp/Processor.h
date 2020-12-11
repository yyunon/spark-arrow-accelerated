//
// Created by Fabian Nonnenmacher on 15.05.20.
//

#ifndef SPARK_EXAMPLE_PROCESSOR_H
#define SPARK_EXAMPLE_PROCESSOR_H

#include <arrow/api.h>
#include <iostream>
#include <utility>
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessor.h"

class Processor {
public:
    Processor(std::vector<std::shared_ptr<arrow::Schema> > schema_)
    : schema(schema_){};
    virtual std::shared_ptr<arrow::RecordBatch> process(std::shared_ptr<arrow::RecordBatch> input) = 0;
    virtual ~Processor() = default;
    std::vector<std::shared_ptr<arrow::Schema> > schema;
};

#endif //SPARK_EXAMPLE_PROCESSOR_H
