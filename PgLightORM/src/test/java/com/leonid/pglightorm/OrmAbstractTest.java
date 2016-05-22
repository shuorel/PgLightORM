/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.leonid.pglightorm;

import java.sql.Connection;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Shuorel
 */
public class OrmAbstractTest {
    static class Orm extends OrmAbstract {
        public static Orm getInstance() {
            return SingletonHolder.INSTANCE;
        }

        private static class SingletonHolder {
            private static final Orm INSTANCE = new Orm();
        }

        private Orm(){}
        
        @Override
        public Connection getConnection() {
            return DbConnectorAbstractTest.DbConnector.getInstance().getConnection();
        }
    }
    
    @Table(schema="index", name="index")
    public class IndexData {
        @Id
        @Column
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
    
    public OrmAbstractTest() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
        DbConnectorAbstractTest.DbConnector.getInstance().close();
    }

    @Test
    public void testSomeMethod() {
        Orm orm = Orm.getInstance();
        IndexData data = new IndexData();
        data.setId("try");
        orm.delete(data);
    }
    
}
