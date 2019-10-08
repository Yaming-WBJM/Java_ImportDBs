import java.io.File;
import java.io.FileReader;    
import java.io.IOException;    
import org.json.JSONException;  
import org.json.JSONObject;  
import org.json.JSONArray; 
import org.json.JSONTokener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;
import org.ini4j.Profile.Section;
  
public class Insert_nba_player_lst {  
  
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
                
            	FileReader reader = new FileReader(  
            			section.get("dir")+"\\NBA\\NBA_Players.json");
                JSONTokener jsrc = new JSONTokener(reader);
                JSONObject dataJson = new JSONObject(jsrc);
                JSONArray features = dataJson.getJSONArray("data");
                for (int i = 0; i < features.length(); i++) {
                	JSONArray info = features.getJSONArray(i);
                	Integer Player_id = info.getInt(0);
                    String NBAname = info.getString(2);
                    String sql = "INSERT INTO nba_players(player_id,full_name) "+ "VALUES ("+Player_id.toString()+",'"+NBAname.replaceAll("\'", "\'\'")+"');";
                    stmt.executeUpdate(sql);                   
                }
                stmt.close();
                c.commit();
                c.close();
            } catch (JSONException e) {   
                e.printStackTrace();  
            } //NBA  */
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
          }
          System.out.println("Table created successfully");   
    	
    }  
  
}