package tech.tablesaw.io.parquet;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestPyArrowParameterized {

    private static Stream<Arguments> columnTypeParameters() {
        return Stream.of(
                Arguments.of(ColumnType.BOOLEAN, 0),
                Arguments.of(ColumnType.INTEGER, 1),
                Arguments.of(ColumnType.INTEGER, 2),
                Arguments.of(ColumnType.INTEGER, 3),
                Arguments.of(ColumnType.INTEGER, 4),
                Arguments.of(ColumnType.INTEGER, 5),
                Arguments.of(ColumnType.INTEGER, 6),
                Arguments.of(ColumnType.LONG, 7),
                Arguments.of(ColumnType.LONG, 8),
                Arguments.of(ColumnType.DOUBLE, 9),
                Arguments.of(ColumnType.DOUBLE, 10),
                Arguments.of(ColumnType.LOCAL_DATE_TIME, 11),
                Arguments.of(ColumnType.STRING, 12)
        );
    }

    private static Stream<Arguments> columnValueParameters() {
        return Stream.of(
                Arguments.of(0, true, false),
                Arguments.of(1, 0, 127),
                Arguments.of(2, -127, 1),
                Arguments.of(3, 0, 32767),
                Arguments.of(4, 0, -32767),
                Arguments.of(5, 0, 65000),
                Arguments.of(6, 0, -65000),
                Arguments.of(7, 0L, 1_000_000_000L),
                Arguments.of(8, 0L, -1_000_000_000L),
                Arguments.of(9, null, 1.0d),
                Arguments.of(10, 0.0d, null),
                Arguments.of(11, LocalDateTime.of(2021, 4, 23, 0, 0), LocalDateTime.of(2021, 4, 23, 0, 0, 1)),
                Arguments.of(12, "string1", "string2")
        );
    }

    private static Table table;

    @BeforeAll
    static void beforeAll() throws IOException {
        table = new TablesawParquetReader()
                .read(TablesawParquetReadOptions
                .builder(new File("target/test-classes/pandas_pyarrow.parquet"))
                .build());
        assertEquals("pandas_pyarrow.parquet", table.name(), "Wrong table name");

    }

    @ParameterizedTest
    @MethodSource("columnTypeParameters")
    void testColumnType(final ColumnType type, final int index) {
        assertEquals(type, table.column(index).type(), String.format("Wrong column type %s at index %d", type.name(), index));
    }

    @ParameterizedTest
    @MethodSource("columnValueParameters")
    void testColumnValues(final int index, final Object value0, Object value1) {
        assertEquals(value0, table.column(index).get(0), String.format("Wrong value at [%d,0]",  index));
        assertEquals(value1, table.column(index).get(1), String.format("Wrong value at [%d,1]",  index));
    }
}
