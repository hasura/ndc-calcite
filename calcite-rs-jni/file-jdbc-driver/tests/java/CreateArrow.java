import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

public class CreateArrow {
    public static void main(String[] args) throws Exception {
        String outputFile = "tests/data/arrow/simple.arrow";
        
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // Create schema
            Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
            Field nameField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field ageField = new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null);
            Field salaryField = new Field("salary", FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
            Field activeField = new Field("active", FieldType.nullable(new ArrowType.Bool()), null);
            
            Schema schema = new Schema(Arrays.asList(idField, nameField, ageField, salaryField, activeField));
            
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                // Get vectors
                BigIntVector idVector = (BigIntVector) root.getVector("id");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
                BigIntVector ageVector = (BigIntVector) root.getVector("age");
                Float8Vector salaryVector = (Float8Vector) root.getVector("salary");
                BitVector activeVector = (BitVector) root.getVector("active");
                
                // Allocate vectors
                root.allocateNew();
                
                // Add data
                idVector.set(0, 1);
                nameVector.set(0, "Alice".getBytes());
                ageVector.set(0, 25);
                salaryVector.set(0, 50000.0);
                activeVector.set(0, 1);
                
                idVector.set(1, 2);
                nameVector.set(1, "Bob".getBytes());
                ageVector.set(1, 30);
                salaryVector.set(1, 60000.0);
                activeVector.set(1, 0);
                
                idVector.set(2, 3);
                nameVector.set(2, "Charlie".getBytes());
                ageVector.set(2, 35);
                salaryVector.set(2, 70000.0);
                activeVector.set(2, 1);
                
                // Set row count
                root.setRowCount(3);
                
                // Write to file
                File file = new File(outputFile);
                try (FileOutputStream fileOutputStream = new FileOutputStream(file);
                     WritableByteChannel channel = fileOutputStream.getChannel();
                     ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
                    
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }
                
                System.out.println("Created uncompressed Arrow file: " + outputFile);
            }
        }
    }
}