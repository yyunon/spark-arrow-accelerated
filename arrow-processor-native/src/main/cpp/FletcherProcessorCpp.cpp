//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#include "FletcherProcessorCpp.h"


long trivialCpuVersion(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    auto strings = std::static_pointer_cast<arrow::StringArray>(record_batch->column(0));
    auto numbers = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(1));

    const int64_t* raw_numbers = numbers->raw_values();

    int64_t sum = 0;
    for (int i = 0; i < record_batch->num_rows(); i++) {

        if (strings->GetString(i) == "Blue Ribbon Taxi Association Inc.") {
            sum += raw_numbers[i];
        }
    }
    return sum;
}
//long FletcherProcessorCpp::reduce(std::shared_ptr<arrow::RecordBatch> record_batch ) {
//
//    // Create a context for our application on the platform.
//   std::shared_ptr<fletcher::Context> context;
//   ASSERT_FLETCHER_OK(fletcher::Context::Make(&context, platform));
//
//   // Queue the recordbatch to our context.
//   ASSERT_FLETCHER_OK(context->QueueRecordBatch(record_batch));
//
//   // "Enable" the context, potentially copying the recordbatch to the device. This depends on your platform.
//   // AWS EC2 F1 requires a copy, but OpenPOWER SNAP doesn't.
//   ASSERT_FLETCHER_OK(context->Enable());
//
//   // Create a kernel based on the context.
//   fletcher::Kernel kernel(context);
//
//   // Reset the kernel.
//   ASSERT_FLETCHER_OK(kernel.Reset());
//
//   // Start the kernel.
//   ASSERT_FLETCHER_OK(kernel.Start());
//
//   // Wait for the kernel to finish.
//   ASSERT_FLETCHER_OK(kernel.PollUntilDone());
//
//   uint32_t return_value_0;
//   uint32_t return_value_1;
//
//   // Obtain the return value.
//   ASSERT_FLETCHER_OK(kernel.GetReturn(&return_value_0, &return_value_1));
//
//   long result = *reinterpret_cast<int64_t *>(&return_value_0);
//
//   std::cout << "RESULT returned from Fletcher: " << *reinterpret_cast<int64_t *>(&return_value_0) << std::endl;
//
//    //The echo platform does not return a proper value -> fallback to cpu impl
//    if (platform->name() == "echo") {
//        return trivialCpuVersion(record_batch);
//    }
//    return result;
//}
//
long FletcherProcessorCpp::join(std::vector<std::shared_ptr<arrow::RecordBatch> > record_batch ) {

    // Create a context for our application on the platform.
   std::shared_ptr<fletcher::Context> context;

   // Queue the recordbatch to our context.
   for(auto batches : record_batch)
     ASSERT_FLETCHER_OK(context->QueueRecordBatch(batches));

   // "Enable" the context, potentially copying the recordbatch to the device. This depends on your platform.
   // AWS EC2 F1 requires a copy, but OpenPOWER SNAP doesn't.
   ASSERT_FLETCHER_OK(context->Enable());

   // Create a kernel based on the context.
   fletcher::Kernel kernel(context);

   // Reset the kernel.
   ASSERT_FLETCHER_OK(kernel.Reset());

   // Start the kernel.
   ASSERT_FLETCHER_OK(kernel.Start());

   // Wait for the kernel to finish.
   ASSERT_FLETCHER_OK(kernel.PollUntilDone());

   uint32_t return_value_0;
   uint32_t return_value_1;

   // Obtain the return value.
   ASSERT_FLETCHER_OK(kernel.GetReturn(&return_value_0, &return_value_1));

   long result = *reinterpret_cast<int64_t *>(&return_value_0);

   std::cout << "RESULT returned from Fletcher: " << *reinterpret_cast<int64_t *>(&return_value_0) << std::endl;

    //The echo platform does not return a proper value -> fallback to cpu impl
    if (platform->name() == "echo") {
        return trivialCpuVersion(record_batch[0]);//Just to fill in!bb
    }
    return result;
}

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor_initFletcherProcessor
        (JNIEnv *env, jobject, jbyteArray schema_arr_1, jbyteArray schema_arr_2) {

    // TODO: Find a more intelligent way 
    jsize schema_len_1 = env->GetArrayLength(schema_arr_1);
    jbyte *schema_bytes_1 = env->GetByteArrayElements(schema_arr_1, 0);
    jsize schema_len_2 = env->GetArrayLength(schema_arr_2);
    jbyte *schema_bytes_2 = env->GetByteArrayElements(schema_arr_2, 0);

    //std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);

    return (jlong) new FletcherProcessorCpp({ReadSchemaFromProtobufBytes(schema_bytes_1, schema_len_1),ReadSchemaFromProtobufBytes(schema_bytes_2, schema_len_2) });
}



/*
 * Class:     nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor
 * Method:    reduce
 * Signature: (JI[J[J)J
 */
JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor_join
        (JNIEnv *env, jobject, jlong process_ptr, jint num_rows_1, jlongArray in_buf_addrs_1, jlongArray in_buf_sizes_1, jint num_rows_2, jlongArray in_buf_addrs_2, jlongArray in_buf_sizes_2) {

    FletcherProcessorCpp *processor = (FletcherProcessorCpp *) process_ptr;

    // Extract input RecordBatch
    int in_buf_len_1 = env->GetArrayLength(in_buf_addrs_1);
    ASSERT(in_buf_len_1 == env->GetArrayLength(in_buf_sizes_1), "mismatch in arraylen of buf_addrs and buf_sizes");
    int in_buf_len_2 = env->GetArrayLength(in_buf_addrs_2);
    ASSERT(in_buf_len_2 == env->GetArrayLength(in_buf_sizes_2), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *in_addrs_1 = env->GetLongArrayElements(in_buf_addrs_1, 0);
    jlong *in_sizes_1 = env->GetLongArrayElements(in_buf_sizes_1, 0);

    jlong *in_addrs_2 = env->GetLongArrayElements(in_buf_addrs_2, 0);
    jlong *in_sizes_2 = env->GetLongArrayElements(in_buf_sizes_2, 0);

    std::vector<std::shared_ptr<arrow::RecordBatch> > in_batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(processor->schema[0], num_rows_1, in_addrs_1, in_sizes_1, in_buf_len_1, &in_batch[0]));
    ASSERT_OK(make_record_batch_with_buf_addrs(processor->schema[1], num_rows_2, in_addrs_2, in_sizes_2, in_buf_len_2, &in_batch[1]));

    return (jlong) processor->join(in_batch);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherProcessor_close
        (JNIEnv *, jobject, jlong process_ptr) {
    delete (FletcherProcessorCpp *) process_ptr;
}
