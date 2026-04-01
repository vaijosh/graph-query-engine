package com.graphqueryengine.mapping;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class MappingStore {
    private final Map<String, StoredMapping> mappings = new ConcurrentHashMap<>();
    private final AtomicReference<String> activeMappingId = new AtomicReference<>();

    public StoredMapping put(String mappingId, String mappingName, MappingConfig config, boolean setActive) {
        validate(config);
        String normalizedId = normalizeId(mappingId);
        String normalizedName = normalizeName(mappingName, normalizedId);
        StoredMapping stored = new StoredMapping(normalizedId, normalizedName, config, Instant.now().toString());
        mappings.put(normalizedId, stored);
        if (setActive || activeMappingId.get() == null) {
            activeMappingId.set(normalizedId);
        }
        return stored;
    }

    public Optional<MappingConfig> get() {
        return getActive().map(StoredMapping::config);
    }

    public Optional<StoredMapping> getActive() {
        String id = activeMappingId.get();
        if (id == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mappings.get(id));
    }

    public Optional<StoredMapping> getById(String id) {
        if (id == null || id.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(mappings.get(id.trim()));
    }

    public List<MappingInfo> list() {
        List<MappingInfo> info = new ArrayList<>();
        String activeId = activeMappingId.get();
        for (StoredMapping stored : mappings.values()) {
            info.add(new MappingInfo(
                    stored.id(),
                    stored.name(),
                    stored.config().vertices().keySet(),
                    stored.config().edges().keySet(),
                    stored.createdAt(),
                    stored.id().equals(activeId)
            ));
        }
        info.sort(Comparator.comparing(MappingInfo::createdAt).reversed());
        return info;
    }

    public boolean setActive(String mappingId) {
        StoredMapping mapping = mappings.get(mappingId);
        if (mapping == null) {
            return false;
        }
        activeMappingId.set(mapping.id());
        return true;
    }

    public Optional<String> activeMappingId() {
        return Optional.ofNullable(activeMappingId.get());
    }

    public boolean isEmpty() {
        return mappings.isEmpty();
    }

    public DeleteResult delete(String mappingId) {
        if (mappingId == null || mappingId.isBlank()) {
            return new DeleteResult(false, "Mapping id must not be blank", null);
        }
        String id = mappingId.trim();
        StoredMapping removed = mappings.remove(id);
        if (removed == null) {
            return new DeleteResult(false, "Mapping not found: " + id, null);
        }
        // If deleted mapping was active, pick another or clear
        String currentActive = activeMappingId.get();
        String newActive = null;
        if (id.equals(currentActive)) {
            // pick the most recently created remaining mapping
            String fallback = mappings.values().stream()
                    .max(Comparator.comparing(StoredMapping::createdAt))
                    .map(StoredMapping::id)
                    .orElse(null);
            activeMappingId.set(fallback);
            newActive = fallback;
        }
        return new DeleteResult(true, null, newActive);
    }

    public record StoredMapping(String id, String name, MappingConfig config, String createdAt) {
    }

    public record MappingInfo(String id,
                              String name,
                              java.util.Set<String> vertexLabels,
                              java.util.Set<String> edgeLabels,
                              String createdAt,
                              boolean active) {
    }

    public record DeleteResult(boolean deleted, String error, String newActiveMappingId) {
    }

    private static void validate(MappingConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Mapping file is empty or invalid");
        }
        if (config.vertices().isEmpty() && config.edges().isEmpty()) {
            throw new IllegalArgumentException("Mapping must define at least one vertex or edge label");
        }
        config.vertices().forEach((label, def) -> {
            if (isBlank(label) || def == null || isBlank(def.table())) {
                throw new IllegalArgumentException("Invalid vertex mapping for label: " + label);
            }
        });
        config.edges().forEach((label, def) -> {
            if (isBlank(label) || def == null || isBlank(def.table())) {
                throw new IllegalArgumentException("Invalid edge mapping for label: " + label);
            }
        });
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String normalizeId(String mappingId) {
        if (mappingId == null || mappingId.isBlank()) {
            return "mapping-" + Instant.now().toEpochMilli();
        }
        return mappingId.trim();
    }

    private static String normalizeName(String mappingName, String defaultId) {
        if (mappingName == null || mappingName.isBlank()) {
            return defaultId;
        }
        return mappingName.trim();
    }
}
