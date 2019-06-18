package application.step;

import application.domaine.Student;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.ClassPathResource;

public class StudentCsvReader implements ItemReader<FlatFileItemReader<Student>> {

    @Override
    public FlatFileItemReader<Student> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return new FlatFileItemReader<Student>() {{
            setResource(new ClassPathResource("student.csv"));
            // configurer File et relation mapping
            setLineMapper(new DefaultLineMapper<Student>() {{
                setLineTokenizer(new DelimitedLineTokenizer() {{
                    setNames("fistName","lastName", "email", "age");
                }});
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {{
                    setTargetType(Student.class);
                }});
            }});
        }};
    }
}
