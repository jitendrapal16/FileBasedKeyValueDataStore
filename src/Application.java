
import org.json.JSONObject;
import org.json.JSONException;

/**
 * Driver program for testing
 *
 */
public class Application {

	public static void main(String a[]) throws Exception {
		DataStore ds= new DataStore("E://DataStore.JSON");  //Pass path for different storage location
		//Adding entries
		ds.create("first", new JSONObject("{  'name' : 'Ashok' }"));
		ds.create("second", new JSONObject("{'email' : 'abc.s@gmail.com'}") );
		ds.create("third",  new JSONObject("{'car' : 'ferari'}") );
		ds.create("fourth", new JSONObject("{'bike' : 'ducati'}") );
		
		//Reading entries
		System.out.println(ds.read("first"));
		System.out.println(ds.read("second"));
		System.out.println(ds.read("third"));
		System.out.println(ds.read("fourth"));
		
		//Deleting an entry
		ds.delete("third");
		
		System.out.println("\nAfter deleting :");
		System.out.println("******************");
		System.out.println(ds.read("first"));
		System.out.println(ds.read("second"));
		System.out.println(ds.read("third"));
		System.out.println(ds.read("fourth"));
		
		//Adding more entries
		
		ds.create("fifth", new JSONObject("{ 'type' : 'car-list', 'list' : {  'car1': 'audi', 'car2': 'benz', 'car3': 'jaguar' } }") );
		ds.create("third", new JSONObject("{'engine' : 'v8'}") );
//		ds.create("fourth", new JSONObject("{'bike' : 'ducati'}") );
		
		//Reading entries
		System.out.println("\nAdded few entries:");
		System.out.println("******************");
		System.out.println(ds.read("first"));
		System.out.println(ds.read("second"));
		System.out.println(ds.read("third"));
		System.out.println(ds.read("fourth"));
		System.out.println(ds.read("fifth"));
		
		System.out.println();
		//Try to add existing key
		ds.create("fourth", new JSONObject("{ 'birds' : ['sparrow', 'parrot' ] }")); // will throw ->  Key 'fourth' already present! Error!
		
		//key length exceed test //Will throw -> Key length exceeded! Error!
		ds.create("This is a list of birds which is a json object consisting an array of list", new JSONObject("{ 'birds' : ['sparrow', 'parrot' ] }"));
		
		//Testing time to live
		ds.create("sixth", new JSONObject("{'somedata' : 'somevalue'}"), 3); //key must not be readable after three seconds
		Thread.sleep(2000); 
		System.out.println(ds.read("sixth"));
		Thread.sleep(2000); //after two more seconds
		System.out.println(ds.read("sixth")); // key not available 
		
		
	}
}
