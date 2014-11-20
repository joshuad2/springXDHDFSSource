package io.pivotal.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.format.datetime.DateFormatter;
import org.springframework.messaging.Message;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={
		"classpath:MeterHdfsFile-Test.xml"})
public class HdfsFileIntegrationTest {

	@Autowired
	HdfsFileIntegration fis;
	
	@Autowired
	FileSystem fs;
	
	static String dateFormat="yyyy MM dd";
	static String pattern="[a-zA-Z_0-9]*[\\.]xml";
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetFilePattern() {
		ExpressionParser parser = new SpelExpressionParser();
		boolean trueValue =
		     parser.parseExpression("'test.xml' matches '[a-zA-Z_0-9]*[\\.]xml'").getValue(Boolean.class);

		assert(trueValue);
	}
	
	@Test
	public void testModificationLessThan(){

		DateFormatter formatter = new DateFormatter(dateFormat);
		Date dt1=new Date();
		Long td=dt1.getTime()+(24*3600000);
		dt1=new Date(td);
		String theDate=formatter.print(dt1, Locale.US);
		
		boolean lt=fis.isModificationLessThan(theDate,
				                              dateFormat,
				                              new Date().getTime());
		assert(lt);
	}
	@Test
	public void testMockFileListLessThanUnique() throws IOException, ParseException{

		DateFormatter formatter = new DateFormatter(dateFormat);
		Date dt1=new Date();
		long td=dt1.getTime()+(24*3600000);
		dt1=new Date(td);
		String theDate=formatter.print(dt1, Locale.US);
		String lt=fis.getFiles(pattern,
				               new MockHdfsList(),
				               dateFormat,
				               null,
				               theDate,
				               "Y");
		assert(lt!=null);
	}
	@Test
	public void testMockFileListLessThan() throws IOException, ParseException{

		DateFormatter formatter = new DateFormatter(dateFormat);
		Date dt1=new Date();
		long td=dt1.getTime()+(24*3600000);
		dt1=new Date(td);
		String theDate=formatter.print(dt1, Locale.US);
		String lt=fis.getFiles(pattern,
				               new MockHdfsList(),
				               dateFormat,
				               null,
				               theDate,
				               "N");
		assert(lt!=null);
	}
	@Test
	public void testMockFileListGreaterThan() throws IOException, ParseException{

		DateFormatter formatter = new DateFormatter(dateFormat);
		Date dt1=new Date();
		long td=dt1.getTime()-(24*3600000);
		dt1=new Date(td);
		String theDate=formatter.print(dt1, Locale.US);
		String lt=fis.getFiles(pattern,
				               new MockHdfsList(),
				               dateFormat,
				               theDate,
				               null,
				               "N");
		assert(lt!=null);
	}
	@Test
	@Ignore
	public void testHDFSFileIntegration(){
		fis.setFilePattern(pattern);
		fis.setInputPath("/data/uiq");
		fis.setFileSystem(fs);
		try {
			Message<String> msg=fis.getFiles();
			System.out.println(msg);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	class MockHdfsList implements RemoteIterator<LocatedFileStatus>{

		public boolean retrieved=false;
		public int cntr=0;
		public MockHdfsList() throws IOException{

		}
		@Override
		public boolean hasNext() throws IOException {
			if (cntr<=5){
			return true;
			}else{
				return false;
			}
		}

		@Override
		public LocatedFileStatus next() throws IOException {
			Long modTime=Calendar.getInstance().getTimeInMillis()-(10*36000);
			Long readTime=Calendar.getInstance().getTimeInMillis();
			Path path=new Path("/test/uiq"+cntr+".xml");
			cntr++;
			FileStatus fs=new FileStatus(0, false, 0, 0, modTime, readTime, null, null, null, path);
			LocatedFileStatus lfs=new LocatedFileStatus(fs, null);
			retrieved=true;
			return lfs;
		}
		
	}
}
