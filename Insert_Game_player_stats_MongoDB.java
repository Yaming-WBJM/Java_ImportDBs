import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;    
import java.io.IOException;    
import org.json.JSONException;  
import org.json.JSONObject;  
import org.json.JSONArray; 
import org.json.JSONTokener;
import com.mongodb.MongoClient;
import com.mongodb.DBObject;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;
  
public class Insert_Game_player_stats_MongoDB {  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {  
    	  
            // To connect to mongodb server
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            // Now connect to your databases
            MongoDatabase db = mongoClient.getDatabase("FantacyDB");
            System.out.println("Connect to database successfully");
            //boolean auth = db.authenticate(myUserName, myPassword);
            //System.out.println("Authentication: "+auth);
            MongoCollection coll = db.getCollection("NBA_Game_player_stats");
            System.out.println("Collection mycol selected successfully");

            	File folder1 = new File("D:\\Sport_JSON\\NBA\\Game_player_stats");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = null;
            	    System.out.println(list1[j]);
					try {
						reader = new FileReader("D:\\Sport_JSON\\NBA\\Game_player_stats\\"+list1[j]);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(reader.toString().trim()=="")
						continue;
            	    JSONTokener jsrc = new JSONTokener(reader);
            	    JSONArray nbaPlayer=new JSONArray(jsrc);
                    if(nbaPlayer.length()>0) {
                    	JSONObject dataJson = nbaPlayer.getJSONObject(0);
                    Document doc = Document.parse("{\"GAME_ID\":\""+dataJson.getString("GAME_ID").toString()+"\",\"STATS\":"+nbaPlayer.toString()+"}");
                    coll.insertOne(doc);}
                    }

            System.out.println("Documents inserted successfully");
    } 
  
}