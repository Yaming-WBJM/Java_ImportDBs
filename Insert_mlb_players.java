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
  
public class Insert_mlb_players {  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {    
    	Connection c = null;
        Statement stmt = null;
        Statement stmt_s = null;
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
        stmt_s = c.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try {  
            	File folder1 = new File(section.get("dir")+"\\MLB");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = new FileReader(  
            			section.get("dir")+"\\MLB\\"+list1[j]);
                    JSONTokener jsrc = new JSONTokener(reader);
                    JSONObject dataJson = new JSONObject(jsrc);
                    JSONArray features = dataJson.getJSONArray("people");
                    for (int i = 0; i < features.length(); i++) {
                    	JSONObject info = features.getJSONObject(i);
                    	ResultSet rs = stmt_s.executeQuery("Select player_id From mlb_players Where player_id="+info.optString("id")+';');
                    	rs.last();
                    	if(rs.getRow()==0) {
                    		System.out.println(list1[j]+"-"+info.optString("id"));
                    		String sql = "Insert Into mlb_players Values("+info.optString("id")+","
                    			+ "'"+ info.getString("fullName").replaceAll("\'", "\'\'")+"',"
                    			+ "'"+ info.getString("firstName").replaceAll("\'", "\'\'")+"',"
                    			+ "'"+ info.getString("lastName").replaceAll("\'", "\'\'")+"',"
                    			+ "'"+ info.optString("primaryNumber","")+"',"
                    			+ "'"+ info.getString("birthDate")+"',"
                    			+ "'"+ info.getString("birthCountry").replaceAll("\'", "\'\'")+"',"
                    			+ "'"+ info.getString("height").replace("\'", "-")+"',"
                    			+ "'"+ info.optString("weight")+"',"
                    			+ "'"+ info.optString("active")+"',"
                    			+ info.getJSONObject("currentTeam").optString("id","0")+","
                    			+ "'"+ info.getJSONObject("primaryPosition").optString("name")+"',"
                    			+ "'"+ info.getString("mlbDebutDate")+"',"
                    			+ "'"+ info.getJSONObject("batSide").optString("description")+"',"
                    			+ "'"+ info.getJSONObject("pitchHand").optString("description")+"');";
                    		stmt.executeUpdate(sql); 
                    	}
                    }
                    c.commit();
            	}
                stmt.close();
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