import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.calcite.adapter.graphql.JsonFlatteningDeserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

//public class JsonFlatteningDeserializer extends StdDeserializer<Object> {
//
//    public JsonFlatteningDeserializer() {
//        this(null);
//    }
//
//    public JsonFlatteningDeserializer(Class<?> vc) {
//        super(vc);
//    }
//
//    @Override
//    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
//        JsonNode node = p.getCodec().readTree(p);
//        if (node.isArray()) {
//            // For arrays, process each element separately
//            List<Object> result = new ArrayList<>();
//            for (JsonNode elem : node) {
//                result.add(flatten(elem, ""));
//            }
//            return result;
//        } else {
//            return flatten(node, "");
//        }
//    }
//
//    private Map<String, Object> flatten(JsonNode node, String prefix) {
//        if (node.isObject()) {
//            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.fields(), Spliterator.ORDERED), false)
//                    .flatMap(entry -> {
//                        String key = entry.getKey();
//                        JsonNode value = entry.getValue();
//                        String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
//
//                        if (value.isObject()) {
//                            return flatten(value, fullKey).entrySet().stream();
//                        } else if (value.isArray()) {
//                            // Don't flatten arrays - keep them as lists
//                            List<Object> arrayResult = new ArrayList<>();
//                            for (JsonNode elem : value) {
//                                if (elem.isObject()) {
//                                    arrayResult.add(extractObject(elem));
//                                } else {
//                                    arrayResult.add(extractValue(elem));
//                                }
//                            }
//                            return Stream.of(Map.entry(key, arrayResult));
//                        } else {
//                            return Stream.of(Map.entry(fullKey, extractValue(value)));
//                        }
//                    })
//                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//        } else {
//            return Map.of("", extractValue(node));
//        }
//    }
//
//    private Map<String, Object> extractObject(JsonNode node) {
//        Map<String, Object> result = new HashMap<>();
//        node.fields().forEachRemaining(entry -> {
//            if (entry.getValue().isObject()) {
//                result.put(entry.getKey(), extractObject(entry.getValue()));
//            } else {
//                result.put(entry.getKey(), extractValue(entry.getValue()));
//            }
//        });
//        return result;
//    }
//
//    private Object extractValue(JsonNode node) {
//        if (node.isTextual()) {
//            return node.asText();
//        } else if (node.isNumber()) {
//            return node.numberValue();
//        } else if (node.isBoolean()) {
//            return node.booleanValue();
//        } else if (node.isNull()) {
//            return null;
//        } else {
//            return node.toString();
//        }
//    }
//}

class JsonFlatteningDeserializerTest {
    @Test
    void testDeserialize() throws IOException {
        String json = "{\n" +
                "  \"albumId\": 1,\n" +
                "  \"title\": \"For Those About To Rock We Salute You\",\n" +
                "  \"artist\": {\n" +
                "    \"name\": \"AC/DC\"\n" +
                "  }\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Object.class, new JsonFlatteningDeserializer(null));
        mapper.registerModule(module);

        Map<String, Object> expected = new HashMap<>();
        expected.put("albumId", 1);
        expected.put("title", "For Those About To Rock We Salute You");
        expected.put("artist.name", "AC/DC");

        Object actual = mapper.readValue(json, Object.class);
        assertEquals(expected, actual);
    }

    @Test
    void testDeserializeNestedObjects() throws IOException {
        String jsonArray = "["
                + "{"
                + "  \"a\": { \"e\": 1 }, \"b\": \"test\""
                + "},"
                + "{"
                + "  \"b\": {"
                + "    \"c\": 1,"
                + "    \"d\": 2"
                + "  }"
                + "}"
                + "]";

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Object.class, new JsonFlatteningDeserializer(null));
        mapper.registerModule(module);

        Map<String, Object> expected1 = new HashMap<>();
        expected1.put("a.e", 1);
        expected1.put("b", "test");

        Map<String, Object> expected2 = new HashMap<>();
        expected2.put("b.c", 1);
        expected2.put("b.d", 2);

        List<Map<String, Object>> expected = new ArrayList<>();
        expected.add(expected1);
        expected.add(expected2);

        Object actual = mapper.readValue(jsonArray, Object.class);
        assertEquals(expected, actual);
    }

    @Test
    void testDeserializeWithArray() throws IOException {
        String json = "{\n" +
                "  \"albumId\": 1,\n" +
                "  \"tracks\": [\n" +
                "    {\"name\": \"Track 1\"},\n" +
                "    {\"name\": \"Track 2\"}\n" +
                "  ]\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Object.class, new JsonFlatteningDeserializer(null));
        mapper.registerModule(module);

        HashMap<String, String> track1 = new HashMap<>();
        track1.put("name", "Track 1");

        HashMap<String, String> track2 = new HashMap<>();
        track2.put("name", "Track 2");

        Map<String, Object> expected = new HashMap<>();
        ArrayList<Object> list = new ArrayList<>();
        list.add(track1);
        list.add(track2);
        expected.put("albumId", 1);
        expected.put("tracks", list);

        Object actual = mapper.readValue(json, Object.class);
        assertEquals(expected, actual);
    }
    @Test
    void testDeserializeGraphQLResponse() throws IOException {
        String json = "{\"data\":{\"albums\":[" +
                "{\"albumId\":1,\"title\":\"For Those About To Rock We Salute You\",\"artist\":{\"name\":\"AC/DC\"}}," +
                "{\"albumId\":2,\"title\":\"Balls to the Wall\",\"artist\":{\"name\":\"Accept\"}}," +
                "{\"albumId\":3,\"title\":\"Restless and Wild\",\"artist\":{\"name\":\"Accept\"}}," +
                "{\"albumId\":4,\"title\":\"Let There Be Rock\",\"artist\":{\"name\":\"AC/DC\"}}," +
                "{\"albumId\":5,\"title\":\"Big Ones\",\"artist\":{\"name\":\"Aerosmith\"}}," +
                "{\"albumId\":6,\"title\":\"Jagged Little Pill\",\"artist\":{\"name\":\"Alanis Morissette\"}}," +
                "{\"albumId\":7,\"title\":\"Facelift\",\"artist\":{\"name\":\"Alice In Chains\"}}," +
                "{\"albumId\":8,\"title\":\"Warner 25 Anos\",\"artist\":{\"name\":\"Antônio Carlos Jobim\"}}," +
                "{\"albumId\":9,\"title\":\"Plays Metallica By Four Cellos\",\"artist\":{\"name\":\"Apocalyptica\"}}," +
                "{\"albumId\":10,\"title\":\"Audioslave\",\"artist\":{\"name\":\"Audioslave\"}}" +
                "]}}";

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Object.class, new JsonFlatteningDeserializer(null));
        mapper.registerModule(module);

        Map<String, Object> expected = new HashMap<>();
        List<Map<String, Object>> albums = new ArrayList<>();

        // Build expected flattened albums list
        albums.add(createAlbum(1, "For Those About To Rock We Salute You", "AC/DC"));
        albums.add(createAlbum(2, "Balls to the Wall", "Accept"));
        albums.add(createAlbum(3, "Restless and Wild", "Accept"));
        albums.add(createAlbum(4, "Let There Be Rock", "AC/DC"));
        albums.add(createAlbum(5, "Big Ones", "Aerosmith"));
        albums.add(createAlbum(6, "Jagged Little Pill", "Alanis Morissette"));
        albums.add(createAlbum(7, "Facelift", "Alice In Chains"));
        albums.add(createAlbum(8, "Warner 25 Anos", "Antônio Carlos Jobim"));
        albums.add(createAlbum(9, "Plays Metallica By Four Cellos", "Apocalyptica"));
        albums.add(createAlbum(10, "Audioslave", "Audioslave"));

        expected.put("data.albums", albums);

        Object actual = mapper.readValue(json, Object.class);
        assertEquals(expected, actual);
    }

    private Map<String, Object> createAlbum(int id, String title, String artistName) {
        Map<String, Object> album = new HashMap<>();
        album.put("albumId", id);
        album.put("title", title);
        album.put("artist.name", artistName);
        return album;
    }
}