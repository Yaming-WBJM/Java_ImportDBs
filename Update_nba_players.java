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
  
public class Update_nba_players {  
  
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
            	File folder1 = new File(section.get("dir")+"\\NBA\\Players");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = new FileReader(  
            			section.get("dir")+"\\NBA\\Players\\"+list1[j]);
                    JSONTokener jsrc = new JSONTokener(reader);
                    JSONObject dataJson = new JSONObject(jsrc);
                    JSONArray features = dataJson.getJSONArray("data");
                    for (int i = 0; i < features.length(); i++) {
                    	JSONArray info = features.getJSONArray(i);
                    	System.out.println(cnvStr(info.getInt(0)));
                    	String schl="";
                    	if (info.isNull(7)) schl="";
                    	else schl=info.getString(7).replaceAll("\'", "\'\'");
                    	String cntr="";
                    	if (info.isNull(8)) cntr="";
                    	else cntr=info.getString(8);
                    	String drnd="";
                    	if (info.isNull(28)) drnd="";
                    	else drnd=info.getString(28);
                    	String num="";
                    	if (info.isNull(29)) num="";
                    	else num=info.getString(29);
                    	String sql = "UPDATE nba_players SET fst_name='"+info.getString(1).replaceAll("\'", "\'\'")+"',"
                    			+ "lst_name='"+info.getString(2).replaceAll("\'", "\'\'")+"',"
                    			+ "birthdate='"+info.getString(6)+"',"
                    			+ "school='"+schl+"',"
                    			+ "country='"+cntr+"',"
                    			+ "height='"+info.getString(10)+"',"
                    			+ "weight='"+info.getString(11)+"',"
                    			+ "season_exp="+cnvStr(info.getInt(12))+","
                    			+ "jersey_no='"+info.getString(13)+"',"
                    			+ "main_position='"+info.getString(14)+"',"
                    			+ "roster_status='"+info.getString(15)+"',"
                    			+ "team_id="+cnvStr(info.getInt(16))+","
                    			+ "from_yr="+cnvStr(info.getInt(22))+","
                    			+ "to_yr="+cnvStr(info.getInt(23))+","
                    			+ "dleague='"+info.getString(24)+"',"
                    			+ "nba_flag='"+info.getString(25)+"',"
                    			+ "games_played='"+info.getString(26)+"',"
                    			+ "draft_yr='"+info.getString(27)+"',"
                    			+ "draft_rnd='"+drnd+"',"
                    			+ "draft_num='"+num+"'"
                    			+ " Where player_id="+cnvStr(info.getInt(0))+";";
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