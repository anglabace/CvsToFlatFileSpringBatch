package application.config;

import application.domaine.Student;
import application.listener.CsvToFlatFileJobCompletionListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import application.step.StudentFormatProcessor;

//@Configuration, this class will be processed by the Spring container to generate bean definitions
//@EnableBatchProcessing, provides a base configuration for building batch jobs by creating the next beans available to be autowired:
@Configuration
@EnableBatchProcessing
public class CsvToFlatFileBatchConfig {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    // ****************begin reader, writer, and processor****************

    //----- Reader ------
    @Bean
    public ItemReader<Student> csvStudentItemReader() {
        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("students.csv"));
        reader.setLinesToSkip(1);
            //configurer reader
        reader.setLineMapper(new DefaultLineMapper<Student>() {{
                setLineTokenizer(new DelimitedLineTokenizer() {{
                    setNames("firstName","lastName", "email", "age");
                }});
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {{
                    setTargetType(Student.class);
                }});
            }});
        return reader;
    }

    // ------ Processor ------
    @Bean
    ItemProcessor<Student, Student> csvStudentFormatProcessor() {
        return new StudentFormatProcessor();
    }

    // ------ Writer ------
    @Bean
    public FlatFileItemWriter<Student> csvItemWriter() {

        BeanWrapperFieldExtractor<Student> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"firstName","lastName", "email", "age"});
        fieldExtractor.afterPropertiesSet();

        DelimitedLineAggregator<Student> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        Resource outputResource = new ClassPathResource("results.csv");

        return new FlatFileItemWriterBuilder<Student>()
                .name("csvItemWriter")
                .resource(outputResource)
                .lineAggregator(lineAggregator)
                .build();
    }
// *************** end reader, writer, and processor ******************

    // ----------  listener ------
    @Bean
    public JobExecutionListener CsvToFlatFileJobListener() {
        return new CsvToFlatFileJobCompletionListener();
    }


    // ------  begin job info ------
    @Bean
    public Step csvToFlatFileStep() {
            return stepBuilderFactory.get("csvToFlatFileStep")
                    .<Student, Student>chunk(1)
                    .reader(csvStudentItemReader())
                    .processor(csvStudentFormatProcessor())
                    .writer(csvItemWriter())
                    .build();
    }


    @Bean
    Job csvFileToFlatFileJob() {
        return jobBuilderFactory.get("csvFileToDatabaseJob")
                .incrementer(new RunIdIncrementer())
                .listener(CsvToFlatFileJobListener())
                .flow(csvToFlatFileStep())
                .end()
                .build();
    }
    //  ------ end job info ------
}
