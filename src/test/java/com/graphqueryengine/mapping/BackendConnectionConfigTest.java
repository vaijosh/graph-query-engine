package com.graphqueryengine.mapping;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class BackendConnectionConfigTest {
    @Test
    void urlIsTrimmed() {
        var cfg = new BackendConnectionConfig("  jdbc:h2:mem:test  ", "sa", "", null);
        assertEquals("jdbc:h2:mem:test", cfg.url());
    }
    @Test
    void nullUrlKeptAsNull() {
        var cfg = new BackendConnectionConfig(null, "sa", "", null);
        assertNull(cfg.url());
    }
    @Test
    void h2UrlWithNullUserDefaultsToSa() {
        var cfg = new BackendConnectionConfig("jdbc:h2:mem:test", null, "", null);
        assertEquals("sa", cfg.user());
    }
    @Test
    void h2UrlWithBlankUserDefaultsToSa() {
        var cfg = new BackendConnectionConfig("jdbc:h2:mem:test", "   ", "", null);
        assertEquals("sa", cfg.user());
    }
    @Test
    void nonH2UrlWithNullUserDefaultsToEmptyString() {
        var cfg = new BackendConnectionConfig("jdbc:trino://localhost:8080/iceberg", null, "", null);
        assertEquals("", cfg.user());
    }
    @Test
    void explicitUserIsTrimmed() {
        var cfg = new BackendConnectionConfig("jdbc:trino://localhost:8080/iceberg", "  admin  ", "", null);
        assertEquals("admin", cfg.user());
    }
    @Test
    void nullPasswordDefaultsToEmpty() {
        var cfg = new BackendConnectionConfig("jdbc:h2:mem:test", "sa", null, null);
        assertEquals("", cfg.password());
    }
    @Test
    void passwordIsTrimmed() {
        var cfg = new BackendConnectionConfig("jdbc:h2:mem:test", "sa", "  secret  ", null);
        assertEquals("secret", cfg.password());
    }
    @Test
    void nullDriverClassKeptAsNull() {
        var cfg = new BackendConnectionConfig("jdbc:h2:mem:test", "sa", "", null);
        assertNull(cfg.driverClass());
    }
    @Test
    void driverClassIsTrimmed() {
        var cfg = new BackendConnectionConfig("jdbc:trino://localhost", "admin", "", "  io.trino.jdbc.TrinoDriver  ");
        assertEquals("io.trino.jdbc.TrinoDriver", cfg.driverClass());
    }
    @Test
    void fullConstructorRoundTrip() {
        var cfg = new BackendConnectionConfig(
                "jdbc:trino://localhost:8080/iceberg", "admin", "pass123", "io.trino.jdbc.TrinoDriver");
        assertEquals("jdbc:trino://localhost:8080/iceberg", cfg.url());
        assertEquals("admin", cfg.user());
        assertEquals("pass123", cfg.password());
        assertEquals("io.trino.jdbc.TrinoDriver", cfg.driverClass());
    }
}
