import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Generate uncompressed Arrow test data compatible with Calcite ArrowSchemaFactory.
 * Based on Calcite's ArrowDataTest approach.
 */
public class GenerateArrowData {
    
    public static void main(String[] args) throws IOException {
        File outputFile = new File("tests/data/arrow/sample.arrow");
        outputFile.getParentFile().mkdirs();
        
        System.out.println("Generating uncompressed Arrow test data to: " + outputFile.getAbsolutePath());
        
        // Create schema matching our test data
        Schema arrowSchema = makeArrowSchema();
        
        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
             RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)) {
            
            // Create writer with NO compression (null parameter)
            ArrowFileWriter arrowFileWriter = 
                new ArrowFileWriter(vectorSchemaRoot, null, fileOutputStream.getChannel());
            
            arrowFileWriter.start();
            
            // Create a batch of test data
            int batchSize = 5;
            vectorSchemaRoot.setRowCount(batchSize);
            
            // Fill in the data
            IntVector idVector = (IntVector) vectorSchemaRoot.getVector("id");
            VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            BigIntVector ageVector = (BigIntVector) vectorSchemaRoot.getVector("age");
            Float8Vector salaryVector = (Float8Vector) vectorSchemaRoot.getVector("salary");
            BitVector activeVector = (BitVector) vectorSchemaRoot.getVector("active");
            
            // Allocate vectors first
            idVector.allocateNew(batchSize);
            nameVector.allocateNew(batchSize * 10, batchSize); // estimate 10 chars per name
            ageVector.allocateNew(batchSize);
            salaryVector.allocateNew(batchSize);
            activeVector.allocateNew(batchSize);
            
            String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve"};
            for (int i = 0; i < batchSize; i++) {
                idVector.set(i, i + 1);
                nameVector.set(i, names[i].getBytes());
                ageVector.set(i, 25 + i * 5);
                salaryVector.set(i, 50000.0 + i * 10000.0);
                activeVector.set(i, i % 2 == 0 ? 1 : 0);
            }
            
            idVector.setValueCount(batchSize);
            nameVector.setValueCount(batchSize);
            ageVector.setValueCount(batchSize);
            salaryVector.setValueCount(batchSize);
            activeVector.setValueCount(batchSize);
            
            arrowFileWriter.writeBatch();
            arrowFileWriter.end();
            arrowFileWriter.close();
        }
        
        System.out.println("Successfully generated uncompressed Arrow test data");
    }
    
    private static Schema makeArrowSchema() {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        
        FieldType intType = FieldType.nullable(new ArrowType.Int(32, true));
        FieldType stringType = FieldType.nullable(new ArrowType.Utf8());
        FieldType longType = FieldType.nullable(new ArrowType.Int(64, true));
        FieldType doubleType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        FieldType booleanType = FieldType.nullable(new ArrowType.Bool());
        
        childrenBuilder.add(new Field("id", intType, null));
        childrenBuilder.add(new Field("name", stringType, null));
        childrenBuilder.add(new Field("age", longType, null));
        childrenBuilder.add(new Field("salary", doubleType, null));
        childrenBuilder.add(new Field("active", booleanType, null));
        
        return new Schema(childrenBuilder.build(), null);
    }
}