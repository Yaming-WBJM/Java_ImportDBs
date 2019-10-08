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
  
public class Update_nba_teams {  
   
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
            	File folder1 = new File(section.get("dir")+"\\NBA\\Teams");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = new FileReader(  
            			section.get("dir")+"\\NBA\\Teams\\"+list1[j]);
                    JSONTokener jsrc = new JSONTokener(reader);
                    JSONObject dataJson = new JSONObject(jsrc);
                    JSONArray features = dataJson.getJSONArray("data");
                    for (int i = 0; i < features.length(); i++) {
                    	JSONArray info = features.getJSONArray(i);
                    	System.out.println(cnvStr(info.getInt(0)));
                    	String schl="";
                    	
                    	String sql = "UPDATE nba_teams SET nickname='"+info.getString(2)+"',"
                    			+ "founded_year="+cnvStr(info.getInt(3))+","
                    			+ "city='"+info.getString(4)+"',"
                    			+ "arena='"+info.getString(5).replaceAll("\'", "\'\'")+"',"
                    			+ "head_coach='"+info.getString(9).replaceAll("\'", "\'\'")+"'"
                    			+ " Where team_id="+cnvStr(info.getInt(0))+";";
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