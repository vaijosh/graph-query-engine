package com.graphqueryengine.config;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
class DatabaseConfigTest {
    @Test
    void inferDriverClassForH2Url() {
        assertEquals("org.h2.Driver", DatabaseConfig.inferDriverClass("jdbc:h2:mem:test"));
    }
    @Test
    void inferDriverClassForTrinoUrl() {
        assertEquals("io.trino.jdbc.TrinoDriver",
                DatabaseConfig.inferDriverClass("jdbc:trino://localhost:8080/iceberg"));
    }
    @Test
    void inferDriverClassForUnknownUrlReturnsEmpty() {
        assertEquals("", DatabaseConfig.inferDriverClass("jdbc:postgresql://localhost/mydb"));
    }
    @Test
    void inferDriverClassForNullReturnsEmpty() {
        assertEquals("", DatabaseConfig.inferDriverClass(null));
    }
    @Test
    void constructorStoresAllFields() {
        DatabaseConfig cfg = new DatabaseConfig(
                "jdbc:h2:mem:test", "sa", "pass", "org.h2.Driver");
        assertEquals("jdbc:h2:mem:test", cfg.url());
        assertEquals("sa", cfg.user());
        assertEquals("pass", cfg.password());
        assertEquals("org.h2.Driver", cfg.driverClass());
    }
}
