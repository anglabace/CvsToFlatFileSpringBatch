package application.step;

import application.domaine.Student;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.ClassPathResource;

import java.util.List;

public class StudentFlatFileWriter implements ItemWriter<Student> {
    @Override
    public void write(List<? extends Student> list) throws Exception {
        new FlatFileItemWriterBuilder<Student>()
                .name("writer")
                .resource(new ClassPathResource("result.csv"))
                .lineAggregator(new DelimitedLineAggregator<Student>() {{
                    setFieldExtractor(new BeanWrapperFieldExtractor<Student>() {{
                        setNames(new String[]{"name", "age"});
                    }});
                }}).build();
    }
}
