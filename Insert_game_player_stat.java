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
  
public class Insert_game_player_stat {  
  
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
            	File folder1 = new File(section.get("dir")+"\\NBA\\Game_player_stats");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = new FileReader(  
            			section.get("dir")+"\\NBA\\Game_player_stats\\"+list1[j]);
                    JSONTokener jsrc = new JSONTokener(reader);
                    JSONObject dataJson = new JSONObject(jsrc);
                    JSONArray features = dataJson.getJSONArray("data");
                    for (int i = 0; i < features.length(); i++) {
                    	JSONArray info = features.getJSONArray(i);
                    	if (info.isNull(26)) continue;
                    	String sql = "INSERT INTO nba_game_player_stat VALUES("
                    			+ "'"+info.getString(0)+"',"
                    			+ info.optString(1)+","
                    			+ info.optString(4)+","
                    			+ "'"+info.getString(6)+"',"
                    			+ "'"+info.optString(8)+"',"
                    			+ info.optString(9)+","
                    			+ info.optString(10)+","
                    			+ info.optString(12)+","
                            	+ info.optString(13)+","
                            	+ info.optString(15)+","
                                + info.optString(16)+","
                                + info.optString(18)+","
                                + info.optString(19)+","
                                + info.optString(21)+","
                                + info.optString(22)+","
                                + info.optString(23)+","
                                + info.optString(24)+","
                                + info.optString(25)+","
                                + info.optString(26)+");";
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

	private static String cnvStr(int int1) {
		Integer tmpInt=int1;
		return tmpInt.toString();
	}  
  
}