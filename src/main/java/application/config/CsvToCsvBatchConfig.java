package application.config;

import application.domaine.Student;
import application.listener.CsvToFlatFileJobCompletionListener;
import application.step.StudentFormatProcessor;
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
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

//@Configuration, this class will be processed by the Spring container to generate bean definitions
//@EnableBatchProcessing, provides a base configuration for building batch jobs by creating the next beans available to be autowired
@Configuration
@EnableBatchProcessing
public class CsvToCsvBatchConfig {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private Resource inputResource = new ClassPathResource("input/students.csv");
    private Resource outputResource = new FileSystemResource("output/results.csv");
    // ****************begin reader, writer, and processor****************

    //----- Reader ------
    @Bean
    ItemReader<Student> csvStudentItemReader() {
        //Create reader instance
        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
        //Declare input resource
        reader.setResource(inputResource);
        //Set number of lines to skips. Use it if file has header rows.
        reader.setLinesToSkip(1);
        //Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper<Student>() {
            {
            //columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                    setNames("firstName","lastName", "email", "age");
                }
                });
            //Set values in objet class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {{
                    setTargetType(Student.class);
                }
                });
            }
        });
        return reader;
    }

    // ------ Processor ------
    @Bean
    ItemProcessor<Student, Student>csvStudentFormatProcessor() {
        return new StudentFormatProcessor();
    }

    // ------ Writer ------
    @Bean
    FlatFileItemWriter<Student> csvItemWriter() {
        //Create writer instance
        FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();

        //Set output file location
        writer.setResource(outputResource);

        //All job repetitions should "append" to same output file
        writer.setAppendAllowed(true);

        //separer chaque attributs par ,
        DelimitedLineAggregator<Student> delLineAgg = new DelimitedLineAggregator<>();
        delLineAgg.setDelimiter(",");

        //Name field values sequence based on object properties
        writer.setLineAggregator(new DelimitedLineAggregator<Student>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Student>() {
                    {
                        setNames(new String[] {"firstName","lastName", "email", "age" });
                    }
                });
            }
        });
        return writer;
    }

// *************** end reader, writer, and processor ******************

    // ----------  listener ------
    @Bean
    JobExecutionListener CsvToFlatFileJobListener() {
        return new CsvToFlatFileJobCompletionListener();
    }


    // ------  begin job info ------
    @Bean
    Step csvToFlatFileStep() {
            return stepBuilderFactory
                    .get("csvToFlatFileStep")
                    .<Student, Student>chunk(5)
                    .reader(csvStudentItemReader())
                    .processor(csvStudentFormatProcessor())
                    .writer(csvItemWriter())
                    .build();
    }


    @Bean
    Job csvFileToFlatFileJob() {
        return jobBuilderFactory
                .get("csvFileToFlatFileJob")
                .incrementer(new RunIdIncrementer())
                .start(csvToFlatFileStep())
                .listener(CsvToFlatFileJobListener())
                .build();
    }
    //  ------ end job info ------
}
