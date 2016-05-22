/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.leonid.pglightorm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Shuorel
 */
public class DbConnectorAbstractTest {
    public static class DbConnector extends DbConnectorAbstract {
        private static class SingletonHolder {
            private static final DbConnector INSTANCE = new DbConnector();
        }

        private DbConnector(){}

        public static DbConnector getInstance() {
            return SingletonHolder.INSTANCE;
        }
        @Override
        public String getCatalog() {
            return "postgres";
        }

        @Override
        public String getHost() {
            return "localhost";
        }

        @Override
        public String getUsername() {
            return "postgres";
        }

        @Override
        public String getPassword() {
            return "password";
        }
        
    }
    public DbConnectorAbstractTest() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
        DbConnector.getInstance().close();
    }

    @Test
    public void testConnect() {
        DbConnector.getInstance().connect();
    }
    
}
