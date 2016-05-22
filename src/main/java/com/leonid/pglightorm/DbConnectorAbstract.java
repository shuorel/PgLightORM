package com.leonid.pglightorm;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Shuorel on 2016/4/19.
 */
public abstract class DbConnectorAbstract {
    Connection connection = null;
    final String JDBC_DRIVER = "org.postgresql.Driver";
    final String PORT = "5432";


    public String getJDBC_driver() {
        return JDBC_DRIVER;
    }
    
    public String getPort() {
        return PORT;
    }
    
    public abstract String getCatalog();

    public abstract String getHost();

    public abstract String getUsername();

    public abstract String getPassword();


    public Connection getConnection() {
        connect();
        return connection;
    }

    public boolean isConnected() {
        return !(connection == null);
    }

    @PostConstruct
    public void connect() {
        try {
            if(connection == null || connection.isClosed() ) {
                Class.forName(getJDBC_driver() );
                String url = "jdbc:postgresql://" + getHost() + ":" + getPort() + "/" + getCatalog();
                String msg = "Connecting to database: " + url;
                Logger.getLogger(this.getClass().getName() ).log(Level.INFO, msg);
                connection = DriverManager.getConnection(url, getUsername(), getPassword() );
            }
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(this.getClass().getName() ).log(Level.SEVERE, null, ex);
        }
    }

    @PreDestroy
    public void close() {
        try {
            if(connection != null){
                connection.close();
                connection = null;
                String msg = "Database closed.";
                Logger.getLogger(this.getClass().getName() ).log(Level.INFO, msg);
            }
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName() ).log(Level.SEVERE, null, ex);
        }
    }
}
