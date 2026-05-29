package org.texttechnologylab.duui.dua.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public sealed interface DUAValue permits DUAValue.NullValue, DUAValue.BooleanValue, DUAValue.IntegerValue,
        DUAValue.LongValue, DUAValue.DoubleValue, DUAValue.StringValue, DUAValue.ReferenceValue,
        DUAValue.ListValue, DUAValue.MapValue {
    Object unwrap();

    record NullValue() implements DUAValue {
        @Override
        public Object unwrap() {
            return null;
        }
    }

    record BooleanValue(boolean value) implements DUAValue {
        @Override
        public Object unwrap() {
            return value;
        }
    }

    record IntegerValue(int value) implements DUAValue {
        @Override
        public Object unwrap() {
            return value;
        }
    }

    record LongValue(long value) implements DUAValue {
        @Override
        public Object unwrap() {
            return value;
        }
    }

    record DoubleValue(double value) implements DUAValue {
        @Override
        public Object unwrap() {
            return value;
        }
    }

    record StringValue(String value) implements DUAValue {
        public StringValue {
            Objects.requireNonNull(value, "value");
        }

        @Override
        public Object unwrap() {
            return value;
        }
    }

    record ReferenceValue(DUAEntityRef<? extends DUAEntity> value) implements DUAValue {
        public ReferenceValue {
            Objects.requireNonNull(value, "value");
        }

        @Override
        public Object unwrap() {
            return value;
        }
    }

    record ListValue(List<DUAValue> value) implements DUAValue {
        public ListValue {
            value = value == null ? List.of() : List.copyOf(value);
        }

        @Override
        public Object unwrap() {
            return value;
        }
    }

    record MapValue(Map<String, DUAValue> value) implements DUAValue {
        public MapValue {
            value = value == null ? Map.of() : Map.copyOf(value);
        }

        @Override
        public Object unwrap() {
            return value;
        }
    }
}
