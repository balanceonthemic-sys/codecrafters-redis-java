
// Define a structure for the Stream entries


class StreamEntry {
    String id;
    java.util.Map<String, String> fields;

    StreamEntry(String id, java.util.Map<String, String> fields) {
        this.id = id;
        this.fields = fields;
    }
}