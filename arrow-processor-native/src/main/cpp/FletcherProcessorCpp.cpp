//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#include "FletcherProcessorCpp.h"
#include <fletcher/api.h>
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "jni/ProtobufSchemaDeserializer.h"

#include <utility>
#include <unistd.h>
#include "utils.h"


FletcherProcessorCpp::FletcherProcessorCpp(std::shared_ptr<arrow::Schema> input_schema) {
    schema = std::move(input_schema);

    // Create a Fletcher platform object, attempting to autodetect the platform.
    ASSERT_FLETCHER_OK(fletcher::Platform::Make(&platform, false));

    // Initialize the platform.
    ASSERT_FLETCHER_OK(platform->Init());


    //assert Schema is OK
    //TODO: 1 column string, the other long, both not null
//    std::shared_ptr<arrow::Field> string_field = schema->field(0);
//    if (string_field->type()->Equals(arrow::StringType()) && !string_field->nullable())

}

uint64_t trivialCpuVersion(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    auto quantity = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));
    auto ext = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(1));
    auto discount = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(2));
    auto shipdate = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(3));

    const int64_t* ship_date_raw = shipdate->raw_values();
    const int64_t* discount_raw = discount->raw_values();
    const int64_t* quantity_raw = quantity->raw_values();
    const int64_t* ext_raw = ext->raw_values();

    const int64_t quantity_constant = float_to_fixed<int64_t>(24.0);
    const int64_t discount_up_constant = float_to_fixed<int64_t>(0.061);
    const int64_t discount_down_constant = float_to_fixed<int64_t>(0.059);

    std::cout << "Constants are: " << quantity_constant << "," << discount_down_constant << ","  << discount_up_constant << "\n";
    double sum = 0;
    std::cout << "Number of rows are: " << record_batch -> num_rows() << "\n";
    for (int i = 0; i < record_batch->num_rows(); ++i) {
      //std::cout << quantity_raw[i] << "," << ext_raw[i] << "," << discount_raw[i] << "," << ship_date_raw[i] << "\n";
      if(quantity_raw[i] < quantity_constant && ship_date_raw[i] < 19950101 && ship_date_raw[i] >= 19940101 && discount_raw[i] <= discount_up_constant && discount_raw[i] >= discount_down_constant)
        sum += fixed_to_float<int64_t>(ext_raw[i]) * fixed_to_float<int64_t>(discount_raw[i]);
    }
    std::cout << "RESULT returned from ECHO: "<< std::fixed << sum << std::endl;
    return float_to_fixed<int64_t>(sum);
}

uint64_t FletcherProcessorCpp::reduce(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    // Create a context for our application on the platform.
   std::shared_ptr<fletcher::Context> context;
   ASSERT_FLETCHER_OK(fletcher::Context::Make(&context, platform));

   // Queue the recordbatch to our context.
   ASSERT_FLETCHER_OK(context->QueueRecordBatch(record_batch));

   // "Enable" the context, potentially copying the recordbatch to the device. This depends on your platform.
   // AWS EC2 F1 requires a copy, but OpenPOWER SNAP doesn't.
   ASSERT_FLETCHER_OK(context->Enable());

   // Create a kernel based on the context.
   fletcher::Kernel kernel(context);

   // Reset the kernel.
   ASSERT_FLETCHER_OK(kernel.Reset());
    //The echo platform does not return a proper value -> fallback to cpu impl
    if (platform->name() == "echo") {
        return trivialCpuVersion(record_batch);
    }

   // Start the kernel.
   ASSERT_FLETCHER_OK(kernel.Start());

   // Wait for the kernel to finish.
   ASSERT_FLETCHER_OK(kernel.WaitForFinish());

   uint32_t return_value_0;
   uint32_t return_value_1;
   uint64_t result;

   // Obtain the return value.
   ASSERT_FLETCHER_OK(kernel.GetReturn(&return_value_0, &return_value_1));

   //long result = *reinterpret_cast<int64_t *>(&return_value_0);
   uint32_t rhigh;
   uint32_t rlow;
   for (int i = 0; i < 2; i++) {
    uint64_t value;
    uint64_t offset = FLETCHER_REG_SCHEMA + 2 * context->num_recordbatches() + 2 * context->num_buffers() + i;
    platform->ReadMMIO64(offset, &value);
    value &= 0xffffffff; //the count registers are 32 bits wide, not 64
    if(i == 0)
      rhigh = (uint32_t) value;
    else
      rlow = (uint32_t) value;
   }
   result = rhigh;
   result = (result << 32) | rlow;
   // Print the return value.
   //std::cout << "Return value: " << fixed_to_float(result) << std::endl;


   return result;
}

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor_initFletcherProcessor
        (JNIEnv *env, jobject, jbyteArray schema_arr) {

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);

    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);

    std::cout << schema -> ToString(true) << "\n";

    return (jlong) new FletcherProcessorCpp(schema);
}



/*
 * Class:     nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor
 * Method:    reduce
 * Signature: (JI[J[J)J
 */
JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor_reduce
        (JNIEnv *env, jobject, jlong process_ptr, jint num_rows, jlongArray in_buf_addrs, jlongArray in_buf_sizes) {

    FletcherProcessorCpp *processor = (FletcherProcessorCpp *) process_ptr;

    // Extract input RecordBatch
    int in_buf_len = env->GetArrayLength(in_buf_addrs);
    ASSERT(in_buf_len == env->GetArrayLength(in_buf_sizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *in_addrs = env->GetLongArrayElements(in_buf_addrs, 0);
    jlong *in_sizes = env->GetLongArrayElements(in_buf_sizes, 0);

    std::shared_ptr<arrow::RecordBatch> in_batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(processor->schema, num_rows, in_addrs, in_sizes, in_buf_len, &in_batch));
    return (jlong) processor->reduce(in_batch);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor_close
        (JNIEnv *, jobject, jlong process_ptr) {
    delete (FletcherProcessorCpp *) process_ptr;
}
