package com.springBatch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import com.springBatch.entity.Customer;
import com.springBatch.repository.CustomerRepository;

import lombok.AllArgsConstructor;

@Configuration
//@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig{

	private CustomerRepository customerRepository;

	@Bean
	public FlatFileItemReader<Customer> reader() {
		return new FlatFileItemReaderBuilder<Customer>().name("coffeeItemReader")
			      .resource(new ClassPathResource("customers.csv"))
			      .delimited()
			      .names(new String[] {"id","firstName", "lastName", "email", "gender", "contact", "country", "dob" })
			      .fieldSetMapper(new BeanWrapperFieldSetMapper<Customer>() {{
			          setTargetType(Customer.class);
			      }})
			      .build();
	}

	@Bean
	public LineMapper<Customer> lineMapper() {
		DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id","firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
		BeanWrapperFieldSetMapper<Customer> mapper = new BeanWrapperFieldSetMapper<>();
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(mapper);
		return lineMapper;
	}

	@Bean
	public JdbcBatchItemWriter<?> itemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Customer>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Customer>())
				.sql("INSERT INTO CUSTOMER_INFO (FIRST_NAME,LAST_NAME,EMAIL,GENDER,CONTACT,COUNTRY,DOB) "
						+ "VALUES(:FIRST_NAME,:LAST_NAME,:EMAIL,:GENDER,:CONTACT,:COUNTRY,:DOB)")
				.dataSource(dataSource).build();
	}

	@Bean
	public CustomerProcessor processor() {
		return new CustomerProcessor();
	}

	@Bean
	public RepositoryItemWriter<Customer> writer() {
		RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
		writer.setRepository(customerRepository);
		writer.setMethodName("save");
		return writer;
	}

	@Bean
	public Step step1(JdbcBatchItemWriter<Customer> itemWriter, JobRepository jobRepository,
			PlatformTransactionManager manager) {
		return new StepBuilder("step1", jobRepository).<Customer, Customer>chunk(10, manager).processor(processor())
				.reader(reader()).writer(itemWriter).build();
	}

	@Bean
	public Job runJob(JdbcBatchItemWriter<Customer> writer, JobRepository jobRepository, Step step,
			PlatformTransactionManager manager) {
		return new JobBuilder("customer", jobRepository).incrementer(new RunIdIncrementer())
				.flow(step1(writer, jobRepository, manager)).end().build();
	}
}
