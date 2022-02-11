package com.example.countriesdemo.config;

import com.example.countriesdemo.data.Countries;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    private final String INSERT_QUERY = "Insert  into newcountries(id ,name) values (?,?)";

    @Bean
    public Job processJob() {

        return jobBuilderFactory.get("processJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }


    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1").<Countries, Countries>chunk(100)
                .reader(reader())
                .writer(writer())
                .build();
    }


    @Bean
    public ItemReader<? extends Countries> reader() {
        return new JdbcCursorItemReaderBuilder<Countries>()
                .dataSource(this.dataSource)
                .sql("select ID, NAME from COUNTRIES")
                .rowMapper(new CoutriesRowMapper())
                .saveState(Boolean.FALSE)
                .build();

    }


    @Bean
    public ItemWriter<? super Countries> writer() {

        return new ItemWriter<Countries>() {
            @Override
            public void write(List<? extends Countries> list) throws Exception {


                jdbcTemplate.batchUpdate(INSERT_QUERY, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        Countries cs = list.get(i);
                        ps.setInt(1, cs.getId());
                        ps.setString(2, cs.getCountries());

                    }

                    @Override
                    public int getBatchSize() {
                        return list.size();
                    }
                });
            }
        };
    }




}
