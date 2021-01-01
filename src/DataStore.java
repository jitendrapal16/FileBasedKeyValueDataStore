import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import org.json.JSONObject;  //dependency

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

public class DataStore {

	private String FILE_STORE_PATH=null;
	private String fileStoreName;
	private String fileStoreDirectory;
	private File fileStore;
	private HashMap<String, Long> keyLocationMap = new HashMap<String, Long>();
	private HashMap<String , Integer> timeToLimitMap = new HashMap<String, Integer>();
	private HashMap<String, Long> keyAddedTime = new HashMap<String, Long>();
	private long currentFilePointer=0;
	
//	private Instrumentation instrument;
		
	public DataStore() throws Exception {		
		this.setDefaultPath();
		this.createStoreFile();
	}
	
	public DataStore(String path) {
		if(path!=null) {
			this.FILE_STORE_PATH=path;
			this.createStoreFile();
		}else {
                    
			try {
				//throw new Exception("Path invalid!");
                            System.out.println("Path invalid!");
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	private synchronized HashMap <String, Long> getEntryMap() {
		return this.keyLocationMap;
	}
	
	private synchronized void setAddedTime(String key){
		this.keyAddedTime.put(key, System.currentTimeMillis());
	}
	
	private synchronized Long getAddedTime(String key) {
		return this.keyAddedTime.get(key);
	}
	
	private synchronized HashMap<String, Integer> getTimeToLimit(){
		return this.timeToLimitMap;
	}
	
	private synchronized Long getCurrentFilePointer() {
		return this.currentFilePointer;
	}
	
	private synchronized void updateCurrentFilePointer(Long pointer) {
		 this.currentFilePointer=pointer;
	}
	
	private synchronized void setDefaultPath() {
		this.fileStoreName="dbStore"+System.currentTimeMillis()+".db";
		
		String osType = System.getProperty("os.name");
		if("Linux".equals(osType)) {
			this.FILE_STORE_PATH="."+File.separator+this.fileStoreName;  //#Todo modify default path
		}
	}
	
	private synchronized void createStoreFile() {
		if(this.FILE_STORE_PATH!=null) {
			this.fileStore = new File(this.FILE_STORE_PATH);
			try {
				fileStore.createNewFile();
				this.fileStoreDirectory = (fileStore.getParent()==null)?".":fileStore.getParent();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}else {			
			try {
				//throw new Exception ("Error! File store not ready/initialized!");
                            System.out.println("Error! File store not ready/initialized!");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private long getSize(Object object)  {
		try {
			return ObjectSizeCalculator.getObjectSize(object); 

		}catch(Exception e) {
			e.printStackTrace();
			return (Long) null;
		}	
	}
	
	private Boolean isValidKey(String key) {
		if(key!=null && key.length()<=32) {
			return true;
		}
		return false;
	}
	
	private Boolean isValidValue(JSONObject value) {
		Long size = getSize(value);
		if(size != null && size<=16000) { 
			return true;
		}		
		return false;
	}
	
	private synchronized Boolean isKeyPresent(String key) {		
		if(this.getEntryMap().containsKey(key)) {
			return true;
		}
		return false;	
	}
	
	private synchronized void mapKeyLocation(String key, Long pointer) {
		this.getEntryMap().put(key, pointer);
	}
	
	private synchronized void writeToStore(String key, JSONObject value) throws Exception {		
		RandomAccessFile fileAccess = new RandomAccessFile(this.FILE_STORE_PATH, "rw");
		this.mapKeyLocation(key, this.getCurrentFilePointer());
		fileAccess.seek(this.getCurrentFilePointer());
		String toBeWritten = key+":"+value+"\n";
		fileAccess.write(toBeWritten.getBytes());
		Long pointerLocation = fileAccess.getFilePointer();
		this.updateCurrentFilePointer(pointerLocation);		
		fileAccess.close();		
	}
	
		
	private synchronized Boolean isFileSizeExceed() {
		File file = new File(this.FILE_STORE_PATH);
		if(file.length()<=1000000000) {
			return false;
		}
		return true;
	}
	
	public Boolean create(String key, JSONObject value) {
				
		if(isFileSizeExceed()) {
			System.out.println("File size limit exceeded! Error!");
			return false;
		}
		if(!isValidKey(key)) {
			System.out.println("Key length exceeded! Error!");
			return false;
		}
		if(!isValidValue(value)) {
			System.out.println("Value size limit exceeded! Error!");
			return false;
		}
		if(isKeyPresent(key)) {
			System.out.println("Key '"+key+"' already present! Error!");
			return false;
		}
		try {
			writeToStore(key,value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return true;
	}
	
	public Boolean create(String key, JSONObject value, int timeToLimit) {
		
		if(create(key,value)) {
			this.getTimeToLimit().put(key, timeToLimit);
			this.setAddedTime(key);
			return true;
		}
		return false;
		
	}
	
	private JSONObject getValueFromKey(Long location, String key) throws Exception {
		RandomAccessFile file = new RandomAccessFile(this.FILE_STORE_PATH,"rw");
		file.seek(location);
		String line = file.readLine();
		line = "{"+line+"}";
		JSONObject entry = new JSONObject(line);
		file.close();
		return (JSONObject) entry.get(key);
	}
	
	private Boolean hasValidTimeToLimit(String key) {
		if(this.timeToLimitMap.get(key)!=null) {
			Long addedTime = this.getAddedTime(key);
			Long totalDur =  addedTime + (this.timeToLimitMap.get(key) * 1000);
			if(totalDur < System.currentTimeMillis()) {
				return false;
			}
			return true;		
		}
		return true;
	}
	
	public JSONObject read(String key) throws Exception  {
		Long location =  this.getEntryMap().get(key);
		if(location!=null && hasValidTimeToLimit(key)) {
			return getValueFromKey(location, key);
		}else {		
			System.out.print("Key '"+key+"' not available! Hence "); 
			return null;
		}		
	}
	
	private synchronized void removeFromMaps(String key) {
		this.getEntryMap().remove(key);
		this.timeToLimitMap.remove(key);
		this.keyAddedTime.remove(key);
	}
	
	public synchronized void delete(String key) throws IOException {
		
		Long keyPointer = this.getEntryMap().get(key);

		if(keyPointer!=null) {
			RandomAccessFile file = new RandomAccessFile(this.FILE_STORE_PATH, "rw");
			String dir = this.fileStoreDirectory;
			
			String tempPath = dir+"/_temp.db";
			File tempFile = new File(tempPath);
			
			RandomAccessFile tempAccess = new RandomAccessFile(tempFile,"rw");
			
			String line=null;
//			Boolean flag=false;
			
			if(keyPointer == file.getFilePointer()) {
				line = file.readLine();
//				flag=true;
			}
			while(  (line=file.readLine())!=null ) {
				Long newPointer = tempAccess.getFilePointer();
				int keyEndIndex= line.indexOf(':');
				String currentKey = line.substring(0, keyEndIndex).trim();
				tempAccess.write((line+"\n").getBytes());
				this.getEntryMap().put(currentKey, newPointer);
				if(keyPointer == file.getFilePointer()) {
					line=file.readLine();
//					flag=true;
				}
			}
			this.updateCurrentFilePointer(tempAccess.getFilePointer());
			file.close();
			tempAccess.close();
			tempFile.renameTo(new File(this.FILE_STORE_PATH));
			this.removeFromMaps(key);
			
			
		}else {
			System.out.println("Key '"+key+"' not present!");
		}
		
		
	}
	
	
	
}
