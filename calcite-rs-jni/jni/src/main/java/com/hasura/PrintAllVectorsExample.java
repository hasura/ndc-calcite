package com.hasura;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;

/**
 * The PrintAllVectorsExample class demonstrates how to read and print all vectors from an Arrow file.
 */
public class PrintAllVectorsExample {
    /**
     * Prints all vectors from an Arrow file.
     *
     * @param pathname the file path of the Arrow file
     * @throws IOException if an I/O error occurs while reading the file
     */
    public static void printFile(String pathname) throws IOException {

        File arrowFile = new File(pathname);
        try (FileInputStream fis = new FileInputStream(arrowFile)) {
            SeekableByteChannel sbc = fis.getChannel();
            RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

            try (ArrowFileReader reader = new ArrowFileReader(sbc, allocator)) {
                final VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Map<Long, Dictionary> dictionaries = reader.getDictionaryVectors();
                for (Field field : root.getSchema().getFields()) {
                    System.out.println("Field Name: " + field.getName() + ", Datatype: " + field.getType());
                }
                while (reader.loadNextBatch()) {
                    for (int row = 0; row < root.getRowCount(); row++) {
                        for (FieldVector vector : root.getFieldVectors()) {
                            String columnName = vector.getField().getName();
                            if (!vector.isNull(row)) {  // check if the value is not null
                                Object value = vector.getObject(row);
                                DictionaryEncoding encoder = vector.getField().getDictionary();
                                if (encoder != null) {
                                    Integer index = (Integer) vector.getObject(row);
                                    Dictionary d = dictionaries.get(encoder.getId());
                                    value = d.getVector().getObject(index);
                                }
                                System.out.println("Row " + row + ", Column " + columnName + ": " + value);
                            } else {
                                System.out.println("Row " + row + ", Column " + columnName + ": null" );
                            }
                        }
                    }
                }
            }
        }
    }
}
