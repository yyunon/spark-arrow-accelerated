import pyarrow as pa

# Create a new field named "number" of type int64 that is not nullable.
number_field = pa.field('vectors', pa.int64(), nullable=False)

field_metadata = {b'fletcher_epc' : b'2'}

number_field = number_field.add_metadata(field_metadata)

# Create a list of fields for pa.schema()
schema_in_fields = [number_field]
schema_out_fields = [number_field]

# Create a new schema from the fields.
schema_in = pa.schema(schema_in_fields)
schema_out = pa.schema(schema_out_fields)

print(schema_in)

# Construct some metadata to explain Fletchgen that it 
# should allow the FPGA kernel to read from this schema.
metadata_in = {b'fletcher_mode': b'read',
            b'fletcher_name': b'BatchIn',
            b'fletcher_epc' : b'3'}

metadata_out = {b'fletcher_mode': b'write',
            b'fletcher_name': b'BatchOut'}

# Add the metadata to the schema
schema_in = schema_in.add_metadata(metadata_in)
schema_out = schema_out.add_metadata(metadata_out)

# Create a list of PyArrow Arrays. Every Array can be seen 
# as a 'Column' of the RecordBatch we will create.
data = [pa.array([1, -3, 3, -14, 5, 10, 12, -50, 6])]

print(data)

# Create a RecordBatch from the Arrays.
recordbatch = pa.RecordBatch.from_arrays(data, schema_in)

# Create an Arrow RecordBatchFileWriter.
writer_in = pa.RecordBatchFileWriter('in.rb', schema_in)

# Write the RecordBatch.
writer_in.write(recordbatch)

# Close the writer.
writer_in.close()

# Serialize schema to file for fletchgen input
serialized_out_schema = schema_out.serialize()
pa.output_stream('out.as').write(serialized_out_schema)
