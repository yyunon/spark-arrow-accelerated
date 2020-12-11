//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#ifndef SPARK_EXAMPLE_FLETCHERPROCESSORCPP_H
#define SPARK_EXAMPLE_FLETCHERPROCESSORCPP_H

#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "jni/ProtobufSchemaDeserializer.h"

#include <utility>
#include <unistd.h>
#include <arrow/api.h>
#include <iostream>
#include <utility>
#include <fletcher/api.h>

#include "nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor.h"

class FletcherProcessorCpp {
    std::shared_ptr<fletcher::Platform> platform;
public:
    std::vector<std::shared_ptr<arrow::Schema> > schema;
    explicit FletcherProcessorCpp(std::vector<std::shared_ptr<arrow::Schema> > input_schema): schema(input_schema)
    {
    // Create a Fletcher platform object, attempting to autodetect the platform.
    ASSERT_FLETCHER_OK(fletcher::Platform::Make(&platform, false));
    // Initialize the platform.
    ASSERT_FLETCHER_OK(platform->Init());
    // Create the context at init this time
   ASSERT_FLETCHER_OK(fletcher::Context::Make(&context, platform));

    //assert Schema is OK
    //TODO: 1 column string, the other long, both not null
//    std::shared_ptr<arrow::Field> string_field = schema->field(0);
//    if (string_field->type()->Equals(arrow::StringType()) && !string_field->nullable())

    };
    ~FletcherProcessorCpp() = default;
    long reduce(std::shared_ptr<arrow::RecordBatch> input);
    long broadcast(std::vector<std::shared_ptr<arrow::RecordBatch> > input);
    long join(std::vector<std::shared_ptr<arrow::RecordBatch> > input);
private:
    std::shared_ptr<fletcher::Context> context;
};


#endif //SPARK_EXAMPLE_FLETCHERPROCESSORCPP_H
