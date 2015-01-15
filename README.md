<p>HdfsFile source for Spring-XD</p>
<p>==============================================================</p>

<p>This source class for HDFS provides the ability to use a generic HDFS file system as the source for a Spring-XD stream.</p>
<p>As is the case for every Spring-XD source you should place the HdfsFile.properties and the HdfsFile.xml in the modules/source directory and the jar file in the lib directory. of Spring-XD.
<p>Available Options</p>
<ul>
<li>inputFilePath -The inputFilePath</li>
<li>dateFormat - The Date Format to use i.e. yyy MM dd</li>
<li>greaterThanDateTime- The date time to test against the file to see if it is greater than</li>
<li>lessThanDateTime -The data time to test against the file to see if it is less than</li>
<li>filePattern = The Regular expression file pattern example [a-zA-Z_0-9]*[\\.]xml
<li>fsname.description - HDFS file system example: hdfs://localhost:8020</li>
<li>fsname - hdfs://localhost:8020</li>
<li>hdfsDir - The local directory where HDFS is located example : /data/1/dfs</li>
<li>uniqueFile - Y if the files should be unique </li>
</ul>

