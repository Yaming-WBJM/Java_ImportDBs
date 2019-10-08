import java.io.File;
import java.io.FileReader;    
import java.io.IOException;    
import org.json.JSONException;  
import org.json.JSONObject;  
import org.json.JSONArray; 
import org.json.JSONTokener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;
  
public class Update_mlb_teams {  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {    
    	Connection c = null;
        Statement stmt = null;
        try {
          Class.forName("org.postgresql.Driver");
          c = DriverManager
             .getConnection("jdbc:postgresql://localhost:5432/sportge",
             "postgres", "4Rfv7ujm");
          System.out.println("Opened database successfully");

        Ini ini = new Ini(new File("conf.ini"));
        Section section = ini.get("system");
        
        c.setAutoCommit(false);
        stmt = c.createStatement();
            try {  
            	File folder1 = new File(section.get("dir")+"\\MLB\\Teams");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = new FileReader(  
            			section.get("dir")+"\\MLB\\Teams\\"+list1[j]);
                    JSONTokener jsrc = new JSONTokener(reader);
                    JSONObject dataJson = new JSONObject(jsrc);
                    JSONArray features = dataJson.getJSONArray("teams");
                    for (int i = 0; i < features.length(); i++) {
                    	JSONObject info = features.getJSONObject(i);
                    	System.out.println(list1[j]);
                    	String sql = "Update mlb_teams Set name='"+ info.getString("name").replaceAll("\'", "\'\'")+"',"
                    		+ "firstYearOfPlay='"+ info.getString("firstYearOfPlay")+"',"
                    		+ "location='"+ info.getString("locationName")+"',"
                    		+ "venue='"+ info.getJSONObject("venue").getString("name")+"',"
                    		+ "league='"+ info.getJSONObject("league").getString("name")+"',"
                    		+ "division='"+ info.getJSONObject("division").getString("name")+"'"
                    		+ " Where team_id="+info.optString("id")+ ";";
                    	stmt.executeUpdate(sql); 
                    }
            	}
                stmt.close();
                c.commit();
                c.close();
            } catch (JSONException e) {   
                e.printStackTrace();
            } 
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
          }
          System.out.println("Table created successfully");   
    	
    }
  
  
}