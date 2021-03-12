package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.exception.NetworkException;
import com.ipiecoles.batch.listener.CommuneCSVImportChunkListener;
import com.ipiecoles.batch.listener.CommuneCSVImportStepListener;
import com.ipiecoles.batch.listener.CommuneCSVItemListener;
import com.ipiecoles.batch.listener.CommunesMissingCoordinatesSkipListener;
import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.processor.CommuneProcessor;
import com.ipiecoles.batch.processor.CommunesMissingCoordinatesItemProcessor;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.backoff.FixedBackOffPolicy;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing

public class CommunesImportBatch {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Value("${importFile.chunkSize}")
    private Integer chunkSize;

    @Value("${importFile.fileName}")
    private String fileName;

    @Bean
    public Tasklet task() {
        return new BatchTasklet();
    }

    @Bean
    public Step stepGetMissingCoordinates(){
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(2000);
        return stepBuilderFactory.get("getMissingCoordinates")
                .<Commune, Commune> chunk(10)
                .reader(communesMissingCoordinatesJpaItemReader())
                .processor(communesMissingCoordinatesItemProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .retryLimit(5)
                .retry(NetworkException.class)
                .backOffPolicy(policy)
                .build();
    }

    @Bean
    public StepExecutionListener communeCSVImportStepListener(){
        return new CommuneCSVImportStepListener();
    }

    @Bean
    public ChunkListener communeCSVImportChunkListener(){
        return new CommuneCSVImportChunkListener();
    }

    @Bean
    public CommuneCSVItemListener communeCSVItemListener(){
        return new CommuneCSVItemListener();
    }
    @Bean
    public CommunesMissingCoordinatesSkipListener communesMissingCoordinatesSkipListener(){
        return new CommunesMissingCoordinatesSkipListener();
    }

    @Bean
    public Step stepImportCSV() {
        return stepBuilderFactory.get("importFile")
                .<CommuneDto, Commune>chunk(chunkSize)
                .reader(myCSVReader())
                .processor(communeCSVToCommuneProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .skipLimit(100)
                .skip(CommuneCSVException.class)
                .skip(FlatFileParseException.class)
                .listener(communesMissingCoordinatesSkipListener())
                .listener(communeCSVImportStepListener())
                .listener(communeCSVImportChunkListener())
                .listener(communeCSVItemListener())
                .listener(communeCSVToCommuneProcessor())
                .build();
    }

    @Bean
    public Step step(Tasklet task) {

        return stepBuilderFactory.get("task")
                .tasklet(task())
                .listener(task())
                .build();
    }

    @Bean
    public JpaPagingItemReader<Commune> communesMissingCoordinatesJpaItemReader(){
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("communesMissingCoordinatesJpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c where c.latitude is null or c.longitude is null")
                .build();
    }

    @Bean
    public CommunesMissingCoordinatesItemProcessor communesMissingCoordinatesItemProcessor(){
        return new CommunesMissingCoordinatesItemProcessor();
    }

    @Bean CommuneProcessor communeCSVToCommuneProcessor() {
        return new CommuneProcessor();
    }

    @Bean
    public FlatFileItemReader<CommuneDto> myCSVReader() {
        return new FlatFileItemReaderBuilder<CommuneDto>()
                .name("myCSVReader").linesToSkip(1)
                .delimited().delimiter(";")
                .names("codeInsee", "nom", "codePostal", "line", "acheminement", "gps")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<CommuneDto>() {{
                    setTargetType(CommuneDto.class);
                }})
                .build();
    }

//    @Bean
//    public JdbcBatchItemWriter<Commune> writerJDBC(DataSource dataSource){
//        return new JdbcBatchItemWriterBuilder<Commune>()
//                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
//                .sql("INSERT INTO COMMUNE(code_insee, nom, code_postal, latitude, longitude) VALUES " +
//                        "(:codeInsee, :nom, :codePostal, :latitude, :longitude)")
//                .dataSource(dataSource).build();
//    }

    @Bean
    public JpaItemWriter<Commune> writerJPA() {
        return new JpaItemWriterBuilder<Commune>().entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public Job importCsvJob(Step step, Step stepImportCSV) {
        return jobBuilderFactory.get("importCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .next(stepImportCSV)
                .end().build();
    }

//    @Bean
//    public Job importCsvJob(Step step) {
//        return jobBuilderFactory.get("importCsvJob")
//                .incrementer(new RunIdIncrementer())
//                .flow(step)
//                .end()
//                .build();
//    }

}
