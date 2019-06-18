package application.step;
import application.domaine.Student;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class StudentFormatProcessor implements ItemProcessor<Student, Student> {

    private static final Logger log = LoggerFactory.getLogger(StudentFormatProcessor.class);

    @Override
    public Student process(final Student student) throws Exception {
        Student studentTransformed = student.transformer();
        log.info("Converting (" + student + ") into (" + studentTransformed + ")");
        return studentTransformed;
    }
}