package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class TupleData {

    private final Map<String, Object> data;
    private final int position;

    public TupleData(Map<String, Object> data, int position) {
        this.data = Collections.unmodifiableMap(Objects.requireNonNull(data));
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
