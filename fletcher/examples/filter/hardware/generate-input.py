import pyarrow as pa

# Create a new field named "number" of type int64 that is not nullable.
number_field = pa.field('number', pa.int64(), nullable=False)
predicate_field = pa.field('string', pa.utf8(), nullable=False)

field_metadata = {b'fletcher_epc' : b'20'}

#number_field_metadata = {b'fletcher_epc' : b'16'}

predicate_field = predicate_field.add_metadata(field_metadata)
#number_field = number_field.add_metadata(number_field_metadata)

# Create a list of fields for pa.schema()
schema_fields = [predicate_field, number_field]

# Create a new schema from the fields.
schema = pa.schema(schema_fields)

# Construct some metadata to explain Fletchgen that it 
# should allow the FPGA kernel to read from this schema.
metadata = {b'fletcher_mode': b'read',
            b'fletcher_name': b'ExampleBatch'}

# Add the metadata to the schema
schema = schema.add_metadata(metadata)

# Create a list of PyArrow Arrays. Every Array can be seen 
# as a 'Column' of the RecordBatch we will create.
data = [pa.array(["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc.", 
	"Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "Blue Ribbon Taxi Association Inc."]), pa.array([11, -200, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 
	11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11, -3, 11])]

#data = [pa.array(["Blue Ribbon Taxi Association Inc.", "Blue Ribbon Taxi Association Inc.", "AAAAAAAAAAAAA"]), pa.array([7, 11, -3])]

# Create a RecordBatch from the Arrays.
recordbatch = pa.RecordBatch.from_arrays(data, schema)

# Create an Arrow RecordBatchFileWriter.
writer = pa.RecordBatchFileWriter('recordbatch.rb', schema)

# Write the RecordBatch.
writer.write(recordbatch)

# Close the writer.
writer.close()

