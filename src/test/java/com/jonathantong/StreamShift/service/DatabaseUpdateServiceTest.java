package com.jonathantong.StreamShift.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DatabaseUpdateServiceTest {

    @Mock
    private JdbcTemplate targetJdbcTemplate;

    @InjectMocks
    private DatabaseUpdateService databaseUpdateService;

    @Test
    void insert_shouldExecuteUpdateOnJdbcTemplate_once() {
        // Arrange
        String tableName = "users";
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("username", "testuser");

        // Act
        databaseUpdateService.insert(tableName, data);

        // Assert
        verify(targetJdbcTemplate, times(1)).update(
                any(String.class),   // The generated INSERT SQL query
                any(Object[].class)  // The array of values
        );
    }

    @Test
    void delete_shouldExecuteCorrectDeleteSql_withWhereClause() {
        // Arrange
        String tableName = "products";
        Map<String, Object> whereClause = new HashMap<>();
        whereClause.put("product_id", 100);

        // Define the exact SQL the service is generating (including quotes)
        String expectedSql = "DELETE FROM \"products\" WHERE \"product_id\" = ?";

        Object[] expectedArgs = new Object[]{100};

        // Act
        databaseUpdateService.delete(tableName, whereClause);

        // Assert
        verify(targetJdbcTemplate, times(1)).update(
                eq(expectedSql),
                eq(expectedArgs) // Verifying the object array is passed
        );
    }
}