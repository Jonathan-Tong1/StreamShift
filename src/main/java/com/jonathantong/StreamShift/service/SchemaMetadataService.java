package com.jonathantong.StreamShift.service;

import com.jonathantong.StreamShift.model.ChangeEvent;
import com.jonathantong.StreamShift.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for managing schema metadata and ensuring target tables exist
 */
@Service
public class SchemaMetadataService {

    private static final Logger logger = LoggerFactory.getLogger(SchemaMetadataService.class);

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;

    // Cache for table metadata to avoid repeated database queries
    private final Map<String, TableMetadata> tableMetadataCache = new ConcurrentHashMap<>();

    @Autowired
    public SchemaMetadataService(
            @Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
            @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
    }





}
