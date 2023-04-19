package sqlancer.stonedb;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;

public class StoneDBGlobalState extends SQLGlobalState<StoneDBOptions, StoneDBSchema>{
	private String socketLocation;

	@Override
	protected StoneDBSchema readSchema() throws SQLException {
		return StoneDBSchema.fromConnection(getConnection(), getDatabaseName());
	}
	
	protected String getSocketLocation() {
		if (socketLocation == null) return "/stonedb56/install/tmp/mysql.sock";
		else return socketLocation;
	}
	
}
