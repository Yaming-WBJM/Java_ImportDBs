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
  
public class Insert_mlb_boxscore {  
  
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
            	File folder1 = new File(section.get("dir")+"\\MLB\\Boxscore");
            	String[] list1 = folder1.list();
            	for (int j = 0; j < list1.length; j++) {
            	    FileReader reader = new FileReader(  
            			section.get("dir")+"\\MLB\\Boxscore\\"+list1[j]);
            	    System.out.println(list1[j]);
                    JSONTokener jsrc = new JSONTokener(reader);
                    JSONObject dataJson = new JSONObject(jsrc);
                    JSONObject feat_teams = dataJson.getJSONObject("teams");
                    String[] arrjo = {"batters","pitchers","bench","bullpen"};
                    String[] arrhv = {"home","away"};
                    for (int l = 0; l < arrhv.length; l++) {
                    	JSONObject feat_hv = feat_teams.getJSONObject(arrhv[l]);
                    	for (int k = 0; k < arrjo.length; k++) {
                    		JSONArray feat_jo = feat_hv.getJSONArray(arrjo[k]);                    		
                    		for (int i = 0; i < feat_jo.length(); i++) {
                    			if (feat_teams.getJSONObject(arrhv[l]).getJSONObject("players").isNull("ID"+feat_jo.optString(i))) continue;
                    			JSONObject infos = feat_teams.getJSONObject(arrhv[l]).getJSONObject("players").getJSONObject("ID"+feat_jo.optString(i));
                    			JSONObject info = infos.getJSONObject("stats").getJSONObject("batting");
                    			String pos = "";
                    			if (!infos.isNull("position")) pos=infos.getJSONObject("position").getString("code");
                    			if (!info.isEmpty()) {
                    				ResultSet rs = stmt_s.executeQuery("Select game_id From mlb_boxscore_batting Where game_id="+list1[j].substring(0, list1[j].length()-5)+" and player_id="+feat_jo.optString(i)+';');
                                	rs.last();
                                	if(rs.getRow()==0) {
                                		String sql = "Insert Into mlb_boxscore_batting Values("+list1[j].substring(0, list1[j].length()-5)+","
                    						+ feat_jo.optString(i)+","
                    						+ feat_teams.getJSONObject(arrhv[l]).getJSONObject("team").optString("id")+","
                    						+ info.optString("flyOuts", "0")+","
                    						+ info.optString("groundOuts", "0")+","
                    						+ info.optString("runs", "0")+","
                    						+ info.optString("doubles", "0")+","
                    						+ info.optString("triples", "0")+","
                    						+ info.optString("homeRuns", "0")+","
                    						+ info.optString("strikeOuts", "0")+","
                    						+ info.optString("baseOnBalls", "0")+","
                    						+ info.optString("hits", "0")+","
                    						+ info.optString("hitByPitch", "0")+","
                    						+ info.optString("atBats", "0")+","
                    						+ info.optString("caughtStealing", "0")+","
                    						+ info.optString("stolenBases", "0")+","
                    						+ info.optString("rbi", "0")+","
                    						+ info.optString("leftOnBase", "0")+","
                    						+ info.optString("sacBunts", "0")+","
                    						+ info.optString("sacFlies", "0")+","
                    						+ "'" + pos +"');";
                                		System.out.println(sql);
                                		stmt.executeUpdate(sql);
                                	}
                    			}
                    			info = infos.getJSONObject("stats").getJSONObject("pitching");
                    			pos = "";
                    			if (!infos.isNull("position")) pos=infos.getJSONObject("position").getString("code");
                    			if (!info.isEmpty()) {
                    				ResultSet rs = stmt_s.executeQuery("Select game_id From mlb_boxscore_pitching Where game_id="+list1[j].substring(0, list1[j].length()-5)+" and player_id="+feat_jo.optString(i)+';');
                                	rs.last();
                                	if(rs.getRow()==0) {
                                		String sql = "Insert Into mlb_boxscore_pitching Values("+list1[j].substring(0, list1[j].length()-5)+","
                    						+ feat_jo.optString(i)+","
                    						+ feat_teams.getJSONObject(arrhv[l]).getJSONObject("team").optString("id")+","
                    						+ info.optString("runs", "0")+","
                    						+ info.optString("homeRuns", "0")+","
                    						+ info.optString("strikeOuts", "0")+","
                    						+ info.optString("baseOnBalls", "0")+","
                    						+ info.optString("hits", "0")+","
                    						+ info.optString("inningPitched", "0")+","
                    						+ info.optString("earnedRuns", "0")+","
                    						+ info.optString("battersFaced", "0")+","
                    						+ info.optString("outs", "0")+","
                    						+ "'" + pos+"');";
                                		System.out.println(sql);
                                		stmt.executeUpdate(sql);
                                	}
                    			}
                    			info = infos.getJSONObject("stats").getJSONObject("fielding");
                    			if (!info.isEmpty()) {
                    				ResultSet rs = stmt_s.executeQuery("Select game_id From mlb_boxscore_fielding Where game_id="+list1[j].substring(0, list1[j].length()-5)+" and player_id="+feat_jo.optString(i)+';');
                                	rs.last();
                                	if(rs.getRow()==0) {
                                		String sql = "Insert Into mlb_boxscore_fielding Values("+list1[j].substring(0, list1[j].length()-5)+","
                    						+ feat_jo.optString(i)+","
                    						+ feat_teams.getJSONObject(arrhv[l]).getJSONObject("team").optString("id")+","
                    						+ info.optString("assists", "0")+","
                    						+ info.optString("putOuts", "0")+","
                    						+ info.optString("errors", "0")+");";
                                		System.out.println(sql);
                                		stmt.executeUpdate(sql);
                                	}
                    			}
                    			c.commit();
                    		}
                    	}
                    } 
            	}
                stmt.close();
                stmt_s.close();
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