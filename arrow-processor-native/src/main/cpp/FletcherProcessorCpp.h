//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#ifndef SPARK_EXAMPLE_FLETCHERPROCESSORCPP_H
#define SPARK_EXAMPLE_FLETCHERPROCESSORCPP_H

#include <arrow/api.h>
#include <iostream>
#include <utility>
#include <fletcher/api.h>

#include "nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor.h"

class FletcherProcessorCpp {
    std::shared_ptr<fletcher::Platform> platform;
public:
    std::shared_ptr<arrow::Schema> schema;
    explicit FletcherProcessorCpp(std::shared_ptr<arrow::Schema> input_schema);
    ~FletcherProcessorCpp() = default;
    uint64_t reduce(const std::shared_ptr<arrow::RecordBatch> &input);
};


#endif //SPARK_EXAMPLE_FLETCHERPROCESSORCPP_H
