package com.jonathantong.StreamShift.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Database configuration for source and target PostgreSQL databases
 */
@Configuration
public class DatabaseConfig {

    // Source Database Configuration
    @Value("${streamshift.source.db.url:jdbc:postgresql://localhost:5432/source_db}")
    private String sourceDbUrl;

    @Value("${streamshift.source.db.username:postgres}")
    private String sourceDbUsername;

    @Value("${streamshift.source.db.password:password}")
    private String sourceDbPassword;

    // Target Database Configuration
    @Value("${streamshift.target.db.url:jdbc:postgresql://localhost:5432/target_db}")
    private String targetDbUrl;

    @Value("${streamshift.target.db.username:postgres}")
    private String targetDbUsername;

    @Value("${streamshift.target.db.password:password}")
    private String targetDbPassword;

    /**
     * Source database DataSource
     */
    @Bean(name = "sourceDataSource")
    public DataSource sourceDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(sourceDbUrl);
        config.setUsername(sourceDbUsername);
        config.setPassword(sourceDbPassword);
        config.setDriverClassName("org.postgresql.Driver");

        // Connection pool settings
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        // Connection validation
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(5000);

        config.setPoolName("SourceDB-Pool");

        return new HikariDataSource(config);
    }

    /**
     * Target database DataSource
     */
    @Bean(name = "targetDataSource")
    @Primary
    public DataSource targetDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(targetDbUrl);
        config.setUsername(targetDbUsername);
        config.setPassword(targetDbPassword);
        config.setDriverClassName("org.postgresql.Driver");

        // Connection pool settings
        config.setMaximumPoolSize(20); // More connections for target (writes)
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        // Connection validation
        config.setConnectionTestQuery("SELECT 1");
        config.setValidationTimeout(5000);

        config.setPoolName("TargetDB-Pool");

        return new HikariDataSource(config);
    }

    /**
     * JdbcTemplate for source database
     */
    @Bean(name = "sourceJdbcTemplate")
    public JdbcTemplate sourceJdbcTemplate() {
        return new JdbcTemplate(sourceDataSource());
    }

    /**
     * JdbcTemplate for target database
     */
    @Bean(name = "targetJdbcTemplate")
    @Primary
    public JdbcTemplate targetJdbcTemplate() {
        return new JdbcTemplate(targetDataSource());
    }
}