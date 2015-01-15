package io.pivotal.hdfs;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.format.datetime.DateFormatter;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

/**
 * Spring-XD source for handling HDFS files.
 * 
 * @author Joshua Davis
 *
 */
public class HdfsFileIntegration 
		implements  InitializingBean {
	
	protected static final Log logger = LogFactory
			.getLog(HdfsFileIntegration.class);

	private String inputPath;
	
	private FileSystem fileSystem;
	
	private String filePattern;
	private String dateTimeLessThan;
	private String dateTimeGreaterThan;
	private String dateFormat;
	private String dateEqualsTo;
	private String uniqueFile;
    private Vector<String> exclusionList=new Vector<String>();

	/**
	 * Assert that mandatory properties (lineAggregator) are set.
	 * 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
	}
	
	public boolean isModificationLessThan(String pDateTimeLessThan, String pFormat, long modTime){
		  DateFormatter formatter = new DateFormatter(pFormat);
		  try {
			Date dt=formatter.parse(pDateTimeLessThan, Locale.US);
			if (modTime<dt.getTime()){
				return true;
			}
		} catch (ParseException e) {
			logger.error("Can't Parse the date");
			logger.error(e);
			return false;
		}
           
		return false;
	}
	
	public boolean isModificationTimeGreater(String pDateTimeGreaterThan, String pFormat, long modTime){
		  DateFormatter formatter = new DateFormatter(pFormat);
		  try {
			Date dt=formatter.parse(pDateTimeGreaterThan, Locale.US);
			if (modTime<dt.getTime()){
				return true;
			}
		} catch (ParseException e) {
			logger.error("Can't Parse the date");
			logger.error(e);
			return false;
		}
		return false;
	}
	
	private boolean inExpression(String value,String expression){
		ExpressionParser parser = new SpelExpressionParser();
		String express="'"+value+"' matches '"+expression+"'";
		boolean val =
			     parser.parseExpression(express).getValue(Boolean.class);
		return val;
	}

	public String getFiles(String pFilePattern,
			               RemoteIterator <LocatedFileStatus> files,
			               String pDateFormat,
			               String pGreaterThanTime,
			               String pLessThanTime,
			               String pUniqueFile) throws IOException{
		//logger.info("getFiles:"+pFilePattern+"\n"+pDateFormat+"\n"+pGreaterThanTime+"\n"+pLessThanTime+"\n");
		LocatedFileStatus status;
		StringBuilder sb=new StringBuilder();
		boolean isUniqueFile=false;
		if (pUniqueFile!=null && pUniqueFile.equals("Y")){
			isUniqueFile=true;
		}
		boolean firstOne=true;
		while (files.hasNext()){
		  status=files.next();
		  long fileTime=0;
		  if (pFilePattern!=null &&
			  this.inExpression(status.getPath().getName(),pFilePattern)
		    ){
			if (!isUniqueFile || (isUniqueFile && getExclusionList().contains(status.getPath().getName()))){
		      fileTime=status.getModificationTime();
		      if (pGreaterThanTime!=null && pDateFormat!=null){
		    	if (!this.isModificationTimeGreater(pGreaterThanTime, pDateFormat, fileTime)){
		    		break;
		    	  }
		        }else{
		    	  if (pLessThanTime!=null && pDateFormat!=null){
		    	    if (!this.isModificationLessThan(pLessThanTime, pDateFormat, fileTime)){
		    		  break;
		    	  }
		    	}
		      }
			}
		    Calendar cal=Calendar.getInstance();
		    cal.setTimeInMillis(fileTime);
		    if (!firstOne){
			  sb.append(",");
		    }else{
			  firstOne=false;
		    }
		    sb.append("{\"absoluteFilePath\":\""+
		          this.getInputPath()+"/"+
				  status.getPath().getName()+"\"}");
		    if (isUniqueFile){
		    	logger.info("Added "+status.getPath().getName()+" to list of files.");
		    	getExclusionList().add(status.getPath().getName());
		    }
		  }
		}
	   return sb.toString();
	}
	
	public Message<String> getFiles() throws FileNotFoundException, IOException{
		Path path=new Path(this.getInputPath());
		RemoteIterator<LocatedFileStatus> files=
				fileSystem.listFiles(path, false);
       
        Message<String> msg=MessageBuilder.
        		withPayload(getFiles(this.getFilePattern(),
        				             files,
        				             this.getDateFormat(),
        				             this.getDateTimeGreaterThan(),
        				             this.getDateTimeLessThan(),
        				             this.getUniqueFile())).
        		build();
        return msg;
	}

	/**
	 * @return the filePattern
	 */
	public String getFilePattern() {
		return filePattern;
	}

	/**
	 * @param filePattern the filePattern to set
	 */
	public void setFilePattern(String pFilePattern) {
		if (pFilePattern!=null && !pFilePattern.equals(""))
		  this.filePattern = pFilePattern;
	}

	/**
	 * @return the dateTimeLessThan
	 */
	public String getDateTimeLessThan() {
		return dateTimeLessThan;
	}

	/**
	 * @param dateTimeLessThan the dateTimeLessThan to set
	 */
	public void setDateTimeLessThan(String pDateTimeLessThan) {
		if (pDateTimeLessThan!=null && !pDateTimeLessThan.equals(""))
		  this.dateTimeLessThan = pDateTimeLessThan;
	}

	/**
	 * @return the dateTimeGreaterThan
	 */
	public String getDateTimeGreaterThan() {
		return dateTimeGreaterThan;
	}

	/**
	 * @param dateTimeGreaerThan the dateTimeGreaterThan to set
	 */
	public void setDateTimeGreaterThan(String pDateTimeGreaterThan) {
		if (pDateTimeGreaterThan!=null && !pDateTimeGreaterThan.equals(""))
		this.dateTimeGreaterThan = pDateTimeGreaterThan;
	}

	/**
	 * @return the dateEqualsTo
	 */
	public String getDateEqualsTo() {
		return dateEqualsTo;
	}

	/**
	 * @param dateEqualsTo the dateEqualsTo to set
	 */
	public void setDateEqualsTo(String pDateEqualsTo) {
		if (pDateEqualsTo!=null && !pDateEqualsTo.equals(""))
		  this.dateEqualsTo = pDateEqualsTo;
	}
	
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String pInputPath) {
		if (pInputPath!=null)
		this.inputPath = pInputPath;
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	/**
	 * @return the dateFormat
	 */
	public String getDateFormat() {
		if (dateFormat!=null && !dateFormat.equals(""))
		  return dateFormat;
		else
		  return null;
	}

	/**
	 * @param dateFormat the dateFormat to set
	 */
	public void setDateFormat(String pDateFormat) {
		if (pDateFormat!=null && !pDateFormat.equals(""))
		  this.dateFormat = pDateFormat;
	}

	public String getUniqueFile() {
		return uniqueFile;
	}

	public void setUniqueFile(String uniqueFile) {
		this.uniqueFile = uniqueFile;
	}

	/**
	 * @return the exclusionList
	 */
	public Vector<String> getExclusionList() {
		return exclusionList;
	}

	/**
	 * @param exclusionList the exclusionList to set
	 */
	public void setExclusionList(Vector<String> exclusionList) {
		this.exclusionList = exclusionList;
	}

}
