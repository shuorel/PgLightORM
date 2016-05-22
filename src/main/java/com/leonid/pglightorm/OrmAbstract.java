/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.leonid.pglightorm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 *
 * @author Shuorel
 */
public abstract class OrmAbstract {

    private final Map<Class, Map<String, Object> > saved_entity_map = new HashMap<>();

    abstract public Connection getConnection();

    public static Object changeAnnotationValue(Annotation annotation, String key, Object newValue) {
        Object handler = Proxy.getInvocationHandler(annotation);
        Field f;
        try {
            f = handler.getClass().getDeclaredField("memberValues");
        } catch (NoSuchFieldException | SecurityException e) {
            throw new IllegalStateException(e);
        }
        f.setAccessible(true);
        Map<String, Object> memberValues;
        try {
            memberValues = (Map<String, Object>) f.get(handler);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        Object oldValue = memberValues.get(key);
        if (oldValue == null || oldValue.getClass() != newValue.getClass()) {
            throw new IllegalArgumentException();
        }
        memberValues.put(key,newValue);
        return oldValue;
    }

    public boolean cacheEntity(Object entity) {
        Class class_type = entity.getClass();
        String id_key = "";
        for(Field field : class_type.getDeclaredFields() ) {
            if (field.isAnnotationPresent(Column.class) && field.isAnnotationPresent(Id.class) ) {
                Object value = null;
                for(Method one : class_type.getMethods() ) {
                    if(one.getName().equals(getGetMethodName(field) ) ){
                        try {
                            value = one.invoke(entity);
                        } catch (IllegalAccessException | InvocationTargetException ex) {
                            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                        }
                        break;
                    }
                }
                if(value == null){
                    try {
                        throw new IllegalAccessException("Id field " + field.getName() + " is null.");
                    } catch (IllegalAccessException ex) {
                        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                    }
                    return false;
                }
                id_key += value.toString() + ";";
            }
        }
        if(id_key.length() < 1){
            return false;
        }
        if(saved_entity_map.containsKey(class_type) ){
            Map<String, Object> got = saved_entity_map.get(class_type);
            got.put(id_key, entity);
        }
        else{
            Map<String, Object> got = new HashMap<>();
            got.put(id_key, entity);
            saved_entity_map.put(class_type, got);
        }
        return true;
    }

    public Object getCachedEntity(Object entity) {
        Class class_type = entity.getClass();
        Map<String, Object> id_map = new HashMap<>();
        for(Field field : class_type.getDeclaredFields() ) {
            if (field.isAnnotationPresent(Column.class) && field.isAnnotationPresent(Id.class) ) {
                Object value = null;
                for(Method one : class_type.getMethods() ) {
                    if(one.getName().equals(getGetMethodName(field) ) ){
                        try {
                            value = one.invoke(entity);
                        } catch (IllegalAccessException | InvocationTargetException ex) {
                            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                        }
                        break;
                    }
                }
                if(value == null){
                    try {
                        throw new IllegalAccessException("Id field " + field.getName() + " is null.");
                    } catch (IllegalAccessException ex) {
                        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                    }
                    return null;
                }
                id_map.put(field.getName(), value);
            }
        }
        return getCachedEntity(class_type, id_map);
    }

    public Object getCachedEntity(Class class_type, Map<String, Object> id_map) {
        Object res = null;
        if(!saved_entity_map.containsKey(class_type) ){
            return null;
        }
        Map<String, Object> got = saved_entity_map.get(class_type);
        if(got.isEmpty() ){
            return null;
        }
        List<Object> ids = new LinkedList<>();
        for(Field field : class_type.getDeclaredFields() ) {
            if (field.isAnnotationPresent(Column.class) && field.isAnnotationPresent(Id.class) ) {
                if(!id_map.containsKey(field.getName() ) ){
                    try {
                        throw new IllegalAccessException("Id field " + field.getName() + " not found.");
                    } catch (IllegalAccessException ex) {
                        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                    }
                    return null;
                }
                Object got_id = id_map.get(field.getName() );
                ids.add(got_id);
            }
        }
        String id_key = "";
        for(Object one : ids){
            id_key += one.toString() + ";";
        }
        res = got.get(id_key);
        return res;
    }

    private String getGetMethodName(Field one) {
        String res;
        String name = one.getName();
        if(one.getType().equals(boolean.class) ){
            res =  "is" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length() );
        }
        else{
            res = "get" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length() );
        }
        return res;
    }

    private String getSetMethodName(Field one) {
        String name = one.getName();
        return "set" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length() );
    }
    
    public boolean fillEntity(Object entity, ResultSet rs) {
        boolean res = false;
        if(rs == null){
            return false;
        }
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            if(rs.next() ){
                for(int i=1; i <= rsmd.getColumnCount(); i++){
                    Object value = rs.getObject(i);
                    if(value == null){
                        continue;
                    }
                    String column_name = rsmd.getColumnLabel(i);
                    Class class_type = entity.getClass();
                    for(Field field : class_type.getDeclaredFields() ){
                        if(field.isAnnotationPresent(Column.class) ){
                            Column col_anno = field.getAnnotation(Column.class);
                            if(col_anno.name().equals(column_name) ){
                                column_name = field.getName();
                                break;
                            }
                        }
                    }
                    try{
                        Field field = class_type.getDeclaredField(column_name);
                        if(field.isAnnotationPresent(Column.class) ){
                            if(value.getClass().equals(java.sql.Timestamp.class) ){
                                Calendar start = Calendar.getInstance();
                                start.setTimeInMillis( ((java.sql.Timestamp)value).getTime() );
                                value = start.getTime();
                            }
                            else
                            if(value.getClass().equals(BigDecimal.class) ){
                                value = ((BigDecimal)value).doubleValue();
                            }
                            for(Method one : class_type.getMethods()) {
                                if(one.getName().equals(getSetMethodName(field) ) ){
                                    one.invoke(entity, value);
                                    break;
                                }
                            }
                        }
                    } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException ex) {
                        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                        return false;
                    }
                    catch (NoSuchFieldException ex) {
                        //System.out.println("Field '" + ex.getLocalizedMessage() + "' not found in " + entity.getClass().getSimpleName() );
                    }
                }
                res = true;
            }
        } catch (SQLException | SecurityException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return res;
    }
    
    public <T> boolean fillEntityList(Class class_type, List<T> list, ResultSet rs) {
        if(rs == null){
            return false;
        }
        if(list == null){
            list = new LinkedList<>();
        }
        try {
            while(!rs.isLast() ){
                Object entity;
                entity = class_type.newInstance();
                if(fillEntity(entity, rs) ){
                    list.add((T)entity);
                }
                else{
                    return false;
                }
            }
        } catch (SQLException | SecurityException | InstantiationException | IllegalAccessException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    public boolean delete(Object entity) {
        return delete(entity, getConnection(), null, null);
    }
    
    public boolean delete(Object entity, Connection conn) {
        return delete(entity, conn, null, null);
    }

    public boolean delete(Object entity, String schema, String table) {
        return delete(entity, getConnection(), schema, table);
    }

    public boolean delete(Object entity, Connection conn, String schema, String table) {
        Class class_type = entity.getClass();
        Annotation table_anno = class_type.getAnnotation(Table.class);
        if(table_anno == null){
            try {
                throw new IllegalAccessException("@Table annotion is not found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
            return false;
        }
        if(schema == null || schema.length() == 0){
            schema = ((javax.persistence.Table)table_anno).schema();
        }
        if(table == null || table.length() == 0){
            table = ((javax.persistence.Table)table_anno).name();
        }
        String sql;
        try {
            sql = getDeleteSql(class_type, entity, schema, table);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        try {
            try (Statement stmt = conn.createStatement()) {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "query: {0}", sql);
                stmt.executeUpdate(sql);
                stmt.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    public boolean insert(Object entity, String schema, String table) {
        return insert(entity, getConnection(), schema, table);
    }

    public boolean insert(Object entity, Connection conn, String schema, String table) {
        Class class_type = entity.getClass();
        Annotation table_anno = class_type.getAnnotation(Table.class);
        if(table_anno == null){
            try {
                throw new IllegalAccessException("@Table annotion is not found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
            return false;
        }
        if(schema == null || schema.length() == 0){
            schema = ((javax.persistence.Table)table_anno).schema();
        }
        if(table == null || table.length() == 0){
            table = ((javax.persistence.Table)table_anno).name();
        }
        /*
        try {
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            Logger.getLogger(Orm.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        */
        String sql;
        try {
            sql = getInsertSql(class_type, entity, schema, table);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        try {
            try (Statement stmt = conn.createStatement()) {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "query: {0}", sql);
                stmt.executeUpdate(sql);
                stmt.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        Field id_field = null;
        for(Field field : class_type.getDeclaredFields()){
            if(field.isAnnotationPresent(Id.class) && field.isAnnotationPresent(GeneratedValue.class) ){
                id_field = field;
                break;
            }
        }
        if(id_field != null){
            String sql2 = "SELECT last_value FROM \""  + schema + "\".\""  + table +  "_id_seq\"";
            try (Statement stmt = conn.createStatement()) {
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, "query: {0}", stmt.toString());
                ResultSet rs = stmt.executeQuery(sql2);
                if(rs.next() ){
                    long id = rs.getLong(1);
                    for(Method one : class_type.getMethods()) {
                        if(one.getName().equals(getSetMethodName(id_field) ) ){
                            one.invoke(entity, id);
                            break;
                        }
                    }
                }
                stmt.close();
            } catch (SQLException | IllegalArgumentException | IllegalAccessException | InvocationTargetException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }
        try {
            if(!conn.getAutoCommit() ){
                conn.commit();
            }
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    public boolean insert(Object entity) {
        return insert(entity, getConnection(), null, null);
    }
    
    public boolean insert(Object entity, Connection conn) {
        return insert(entity,  conn, null, null);
    }

    public boolean upsert(Object entity) {
        return upsert(entity, getConnection(), null, null);
    }
    
    public boolean upsert(Object entity, Connection conn) {
        return upsert(entity, conn, null, null);
    }
    
    public boolean upsert(Object entity, Connection conn, String schema, String table) {
        Class class_type = entity.getClass();
        Annotation table_anno = class_type.getAnnotation(Table.class);
        if(table_anno == null){
            try {
                throw new IllegalAccessException("@Table annotion is not found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
            return false;
        }
        
        if(schema == null || schema.length() == 0){
            schema = ((javax.persistence.Table)table_anno).schema();
        }
        if(table == null || table.length() == 0){
            table = ((javax.persistence.Table)table_anno).name();
        }
        
        String sql = "select count(*) from \"";
        if(schema == null || schema.length() == 0){
            sql += table + "\" where ";
        }
        else{
            sql += schema + "\".\"" + table + "\" where ";
        }
        List<NameValueType> id_field = new LinkedList<>();
        for(Field field : class_type.getDeclaredFields()){
            if(!field.isAnnotationPresent(Column.class) ){
                continue;
            }
            if(field.isAnnotationPresent(Id.class) ){
                NameValueType one = new NameValueType();
                Object value = null;
                boolean method_found = false;
                for(Method method : class_type.getMethods()) {
                    if(method.getName().equals(getGetMethodName(field) ) ){
                        try {
                            value = method.invoke(entity);
                        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                            return false;
                        }
                        method_found = true;
                        break;
                    }
                }
                if(!method_found){
                    continue;
                }
                if(value == null){
                    continue;
                }
                String column_name = field.getAnnotation(Column.class).name();
                if(column_name == null || column_name.length() == 0){
                    one.name = field.getName();
                }
                else{
                    one.name = column_name;
                }
                Class type = field.getType();
                if(type.equals(java.util.Date.class) || type.equals(Calendar.class) ){
                    if(field.isAnnotationPresent(Temporal.class) ){
                        TemporalType temporalType = field.getAnnotation(Temporal.class).value();
                        if(temporalType != null) {
                            if(temporalType.equals(TemporalType.DATE) ){
                                if(type.equals(Calendar.class) ){
                                    value = ((Calendar)value).getTime();
                                }
                                type = java.util.Date.class;
                            }
                            else
                            if(temporalType.equals(TemporalType.TIMESTAMP) || temporalType.equals(TemporalType.TIME) ){
                                if(type.equals(java.util.Date.class) ){
                                    Calendar _value = Calendar.getInstance();
                                    _value.setTime((java.util.Date)value);
                                    value = _value;
                                }
                                type = Calendar.class;
                            }
                        }
                    }
                }
                one.value = value;
                one.type = type;
                id_field.add(one);
            }
        }
        if(id_field.isEmpty() ){
            try {
                throw new IllegalAccessException("@Id annotion is not found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }
        for(NameValueType one : id_field){
            sql += one.name + " = ";
            if(one.type.equals(String.class) ){
                sql += "'" + one.value + "' and ";
            }
            else
            if(one.type.equals(java.util.Date.class) ){
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date)one.value);
                sql += "to_date('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                        + "', '" + date_format + "') and ";
            }
            else
            if(one.type.equals(Calendar.class) ){
                Calendar cal = (Calendar)one.value;
                sql += "to_timestamp('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                    + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND) + "', '" + timestamp_format + "') and ";
            }
            else{
                sql += one.value + " and ";
            }
        }
        sql = sql.substring(0, sql.length() - 5);
        int count = 0;
        //System.out.println("sql: " + sql);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql) ) {
            if(rs.next() ){
                count = rs.getInt(1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        if(count == 0){
            return insert(entity, conn, schema, table);
        }
        else{
            return update(entity, conn, schema, table);
        }
    }

    public boolean update(Object entity) {
        return update(entity, getConnection(), null, null);
    }
    
    public boolean update(Object entity, Connection conn) {
        return update(entity, conn, null, null);
    }
    
    public boolean update(Object entity, Connection conn, String schema, String table) {
        Class class_type = entity.getClass();
        Annotation table_anno = class_type.getAnnotation(Table.class);
        if(table_anno == null){
            try {
                throw new IllegalAccessException("@Table annotion is not found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
            return false;
        }
        if(schema == null || schema.length() == 0){
            schema = ((javax.persistence.Table)table_anno).schema();
        }
        if(table == null || table.length() == 0){
            table = ((javax.persistence.Table)table_anno).name();
        }
        
        String sql;
        try {
            sql = getUpdateSql(class_type, entity, schema, table);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        if(sql == null){
            return false;
        }

        try (Statement stmt = conn.createStatement()) {
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, "query: {0}", sql);
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
    
    public boolean associate(Object entity, Class target_type) {
        Class source_type = entity.getClass();
        Field got_field = null;
        for(Field field : source_type.getDeclaredFields()){
            if(field.getGenericType().getTypeName().contains(target_type.getName() ) ){
                got_field = field;
                break;
            }
        }
        if(got_field == null) {
            try {
                throw new IllegalAccessException("no field found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }
        if(got_field.isAnnotationPresent(OneToOne.class) ){
            return oneToOne(entity, target_type, got_field);
        }
        else if(got_field.isAnnotationPresent(ManyToOne.class) ){
            return manyToOne(entity, target_type, got_field);
        }
        else if(got_field.isAnnotationPresent(OneToMany.class) ){
            return oneToMany(entity, target_type, got_field);
        }
        else {
            try {
                throw new IllegalAccessException("no annotion found.");
            } catch (IllegalAccessException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }
    }
    
    private boolean manyToOne(Object entity, Class target_type, Field got_field) {
        return oneToOne(entity, target_type, got_field);
    }
    
    private boolean oneToOne(Object entity, Class target_type, Field got_field) {
        Class source_type = entity.getClass();
        String set_method_name = getSetMethodName(got_field);
        JoinColumn joinColumn = got_field.getAnnotation(JoinColumn.class);
        Map<String, String> field_name_map = new HashMap<>();
        
        if(joinColumn == null) {
            if(got_field.getAnnotation(JoinColumns.class) == null) {
                try {
                    throw new IllegalAccessException("@JoinColumn annotion is not found.");
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                    return false;
                }
            }
            JoinColumn[] colums = got_field.getAnnotation(JoinColumns.class).value();
            for(JoinColumn one : colums) {
                String field_name = one.name();
                String referenced_field_name = one.referencedColumnName();
                if(field_name != null && field_name.length() > 0) {
                    if(referenced_field_name == null || referenced_field_name.length() == 0) {
                        referenced_field_name = field_name;
                    }
                    field_name_map.put(field_name, referenced_field_name);
                }
            }
        }
        else {
            String field_name = joinColumn.name();
            String referenced_field_name = joinColumn.referencedColumnName();
            if(field_name != null && field_name.length() > 0) {
                if(referenced_field_name == null || referenced_field_name.length() == 0) {
                    referenced_field_name = field_name;
                }
                field_name_map.put(field_name, referenced_field_name);
            }
        }
        Map<Field, Object> field_value_map = new HashMap<>();
        try {
            for(String field_name : field_name_map.keySet() ) {
                Field fk_field = source_type.getDeclaredField(field_name);
                Method fk_field_method = source_type.getMethod(getGetMethodName(fk_field), (Class[]) null) ;
                Field referenced_field = target_type.getDeclaredField(field_name_map.get(field_name) );
                Object fk_value = fk_field_method.invoke(entity, (Object[]) null);
                if(fk_value == null){
                    throw new IllegalAccessException("fk_value is null.");
                }
                field_value_map.put(referenced_field, fk_value);
            }
            
            Annotation ano_table = target_type.getAnnotation(Table.class);
            if(ano_table == null){
                throw new IllegalAccessException("@Table annotion is not found.");
            }
            String schema = ((javax.persistence.Table)ano_table).schema();
            if(schema == null || schema.length() == 0) {
                schema = "public";
            }
            String table = ((javax.persistence.Table)ano_table).name();
            String query = "select * from \"" + schema + "\".\"" + table + "\" where ";
            for(Field referenced_field : field_value_map.keySet() ) {
                query += "\"" + referenced_field.getName() + "\" = ? and ";
            }
            query = query.substring(0, query.length() - 4);
            Connection conn = getConnection();
            try (PreparedStatement stmt = conn.prepareStatement(query) ) {
                int count = 0;
                for(Field referenced_field : field_value_map.keySet() ) {
                    count ++;
                    Object value = field_value_map.get(referenced_field);
                    if(value instanceof Date) {
                        stmt.setDate(count,new java.sql.Date(((Date)value).getTime() ) );
                    }
                    else if(value instanceof Calendar) {
                        stmt.setTimestamp(count,new java.sql.Timestamp(((Calendar)value).getTimeInMillis() ) );
                    }
                    else {
                        stmt.setObject(count,value);
                    }
                }
                //Logger.getLogger(Orm.class.getName()).log(Level.INFO, "query: {0}", query);
                ResultSet rs = stmt.executeQuery();
                Object res = target_type.newInstance();
                if(fillEntity(res, rs) ) {
                    Method set_method = source_type.getMethod(set_method_name, target_type);
                    if(set_method == null) {
                        throw new IllegalAccessException("set_method is not found.");
                    }
                    set_method.invoke(entity, res);
                }
            } 
        } catch (NoSuchFieldException | SecurityException | NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SQLException | InstantiationException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
    
    private boolean oneToMany(Object entity, Class target_type, Field got_field) {
        Class source_type = entity.getClass();
        String set_method_name = getSetMethodName(got_field);
        JoinColumn joinColumn = got_field.getAnnotation(JoinColumn.class);
        Map<String, String> field_name_map = new HashMap<>();
        if(joinColumn == null) {
            if(got_field.getAnnotation(JoinColumns.class) == null) {
                try {
                    throw new IllegalAccessException("@JoinColumn annotion is not found.");
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                    return false;
                }
            }
            JoinColumn[] colums = got_field.getAnnotation(JoinColumns.class).value();
            for(JoinColumn one : colums) {
                String field_name = one.name();
                String referenced_field_name = one.referencedColumnName();
                if(field_name != null && field_name.length() > 0) {
                    if(referenced_field_name == null || referenced_field_name.length() == 0) {
                        referenced_field_name = field_name;
                    }
                    field_name_map.put(field_name, referenced_field_name);
                }
            }
        }
        else {
            String field_name = joinColumn.name();
            String referenced_field_name = joinColumn.referencedColumnName();
            if(field_name != null && field_name.length() > 0) {
                if(referenced_field_name == null || referenced_field_name.length() == 0) {
                    referenced_field_name = field_name;
                }
                field_name_map.put(field_name, referenced_field_name);
            }
        }
        Map<Field, Object> field_value_map = new HashMap<>();
        try {
            for(String field_name : field_name_map.keySet() ) {
                Field fk_field = source_type.getDeclaredField(field_name);
                Method fk_field_method = source_type.getMethod(getGetMethodName(fk_field), (Class[]) null) ;
                Field referenced_field = target_type.getDeclaredField(field_name_map.get(field_name) );
                Object fk_value = fk_field_method.invoke(entity, (Object[]) null);
                if(fk_value == null){
                    throw new IllegalAccessException("fk_value is null.");
                }
                field_value_map.put(referenced_field, fk_value);
            }
            
            Annotation ano_table = target_type.getAnnotation(Table.class);
            if(ano_table == null){
                throw new IllegalAccessException("@Table annotion is not found.");
            }
            String schema = ((javax.persistence.Table)ano_table).schema();
            if(schema == null || schema.length() == 0) {
                schema = "public";
            }
            String table = ((javax.persistence.Table)ano_table).name();
            String query = "select * from \"" + schema + "\".\"" + table + "\" where ";
            for(Field referenced_field : field_value_map.keySet() ) {
                query += "\"" + referenced_field.getName() + "\" = ? and ";
            }
            query = query.substring(0, query.length() - 4);
            Connection conn = getConnection();
            try (PreparedStatement stmt = conn.prepareStatement(query) ) {
                int count = 0;
                for(Field referenced_field : field_value_map.keySet() ) {
                    count ++;
                    Object value = field_value_map.get(referenced_field);
                    if(value instanceof Date) {
                        stmt.setDate(count,new java.sql.Date(((Date)value).getTime() ) );
                    }
                    else if(value instanceof Calendar) {
                        stmt.setTimestamp(count,new java.sql.Timestamp(((Calendar)value).getTimeInMillis() ) );
                    }
                    else {
                        stmt.setObject(count,value);
                    }
                }
                //Logger.getLogger(Orm.class.getName()).log(Level.INFO, "query: {0}", query);
                ResultSet rs = stmt.executeQuery();
                List<Object> got_list = new LinkedList<>();
                if(fillEntityList(target_type, got_list, rs) ) {
                    Method set_method = source_type.getMethod(set_method_name, List.class);
                    if(set_method == null) {
                        throw new IllegalAccessException("set method is not found.");
                    }
                    set_method.invoke(entity, got_list);
                }
            } 
        } catch (NoSuchFieldException | SecurityException | NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SQLException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
    
    final public String timestamp_format = "YYYY-MM-DD HH24:MI:SS";
    final public String date_format = "YYYY-MM-DD";

    class ValueType {
        Object value;
        Class type;
    }

    private String getInsertSql(Class class_type, Object entity, String schema, String table) throws IllegalAccessException {
        String sql = "insert into ";
        if(table == null || table.length() == 0){
            throw new IllegalAccessException("name value in @Table annotion is not found.");
        }
        if(schema == null || schema.length() == 0){
            sql += "\"" + table + "\" (";
        }
        else{
            sql += "\"" + schema + "\".\"" + table + "\" (";
        }
        
        List<ValueType> store = new LinkedList<>();
        for(Field field : class_type.getDeclaredFields()){
            try {
                if(!field.isAnnotationPresent(Column.class) ){
                    continue;
                }
                if(field.isAnnotationPresent(GeneratedValue.class) ){
                    continue;
                }
                boolean nullable = field.getAnnotation(Column.class).nullable();
                Object value = null;
                boolean method_found = false;
                for(Method one : class_type.getMethods()) {
                    if(one.getName().equals(getGetMethodName(field) ) ){
                        value = one.invoke(entity);
                        method_found = true;
                        break;
                    }
                }
                if(!method_found){
                    continue;
                }
                if(value == null && !nullable){
                    continue;
                }
                String column_name = field.getAnnotation(Column.class).name();
                if(column_name == null || column_name.length() == 0){
                    sql += "\"" + field.getName() + "\", ";
                }
                else{
                    sql += "\"" + column_name + "\", ";
                }
                Class type = field.getType();
                if(type.equals(java.util.Date.class) || type.equals(Calendar.class) ){
                    if(field.isAnnotationPresent(Temporal.class) ){
                        TemporalType temporalType = field.getAnnotation(Temporal.class).value();
                        if(temporalType != null) {
                            if(temporalType.equals(TemporalType.DATE) ){
                                if(type.equals(Calendar.class) ){
                                    if (value != null) {
                                        value = ((Calendar)value).getTime();
                                    }
                                }
                                type = java.util.Date.class;
                            }
                            else
                            if(temporalType.equals(TemporalType.TIMESTAMP) || temporalType.equals(TemporalType.TIME) ){
                                if(type.equals(java.util.Date.class) ){
                                    Calendar _value = Calendar.getInstance();
                                    if (value != null) {
                                        _value.setTime((java.util.Date)value);
                                    }
                                    value = _value;
                                }
                                type = Calendar.class;
                            }
                        }
                    }
                }
                ValueType one = new ValueType();
                one.value = value;
                one.type = type;
                store.add(one);
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
        }
        sql = sql.substring(0, sql.length() - 2);
        sql += ") values(";
        for(ValueType one : store){
            if(one.value == null){
                sql += "null, ";
            }
            else{
                if(one.type.equals(String.class) ){
                sql += "'" + one.value + "', ";
                }
                else
                if(one.type.equals(java.util.Date.class) ){
                    Calendar cal = Calendar.getInstance();
                    cal.setTime((java.util.Date)one.value);
                    sql += "to_date('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                            + "', '" + date_format + "'), ";
                }
                else
                if(one.type.equals(Calendar.class) ){
                    Calendar cal = (Calendar)one.value;
                    sql += "to_timestamp('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                        + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND) + "', '" + timestamp_format + "'), ";
                }
                else{
                    sql += one.value + ", ";
                }
            }
        }
        sql = sql.substring(0, sql.length() - 2);
        sql += ")";
        return sql;
    }
    
    class NameValueType {
        String name;
        Object value;
        Class type;
    }
    
    private String getDeleteSql(Class class_type, Object entity, String schema, String table) throws IllegalAccessException {
        String sql = "delete from ";
        if(table == null || table.length() == 0){
            throw new IllegalAccessException("name value in @Table annotion is not found.");
        }
        if(schema == null || schema.length() == 0){
            sql += "\"" + table + "\" ";
        }
        else{
            sql += "\"" + schema + "\".\"" + table + "\" ";
        }
        List<NameValueType> id_field = new LinkedList<>();
        for(Field field : class_type.getDeclaredFields() ){
            try {
                if(!field.isAnnotationPresent(Column.class) ){
                    continue;
                }
                boolean nullable = field.getAnnotation(Column.class).nullable();
                Object value = null;
                boolean method_found = false;
                for(Method one : class_type.getMethods() ) {
                    if(one.getName().equals(getGetMethodName(field) ) ){
                        value = one.invoke(entity);
                        method_found = true;
                        break;
                    }
                }
                if(!method_found){
                    continue;
                }
                if(value == null && !nullable){
                    continue;
                }
                if(value == null){
                    continue;
                }
                NameValueType one = new NameValueType();
                String column_name = field.getAnnotation(Column.class).name();
                if(column_name == null || column_name.length() == 0){
                    one.name = field.getName();
                }
                else{
                    one.name = column_name;
                }
                Class type = field.getType();
                if(type.equals(java.util.Date.class) || type.equals(Calendar.class) ){
                    if(field.isAnnotationPresent(Temporal.class) ){
                        TemporalType temporalType = field.getAnnotation(Temporal.class).value();
                        if(temporalType != null) {
                            if(temporalType.equals(TemporalType.DATE) ){
                                if(type.equals(Calendar.class) ){
                                    value = ((Calendar)value).getTime();
                                }
                                type = java.util.Date.class;
                                
                            }
                            else
                            if(temporalType.equals(TemporalType.TIMESTAMP) || temporalType.equals(TemporalType.TIME) ){
                                if(type.equals(java.util.Date.class) ){
                                    Calendar _value = Calendar.getInstance();
                                    _value.setTime((java.util.Date)value);
                                    value = _value;
                                }
                                type = Calendar.class;
                            }
                        }
                    }
                }
                one.value = value;
                one.type = type;
                if(field.isAnnotationPresent(Id.class) ){
                    id_field.add(one);
                }
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
        }
        if(id_field.isEmpty() ){
            throw new IllegalAccessException("@Id annotion is not found.");
        }
        sql += " where ";
        for(NameValueType one : id_field){
            sql += one.name + " = ";
            if(one.type.equals(String.class) ){
                sql += "'" + one.value + "' and ";
            }
            else
            if(one.type.equals(java.util.Date.class) ){
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date)one.value);
                sql += "to_date('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                        + "', '" + date_format + "') and ";
            }
            else
            if(one.type.equals(Calendar.class) ){
                Calendar cal = (Calendar)one.value;
                sql += "to_timestamp('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                    + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND) + "', '" + timestamp_format + "') and ";
            }
            else{
                sql += one.value + " and ";
            }
        }
        sql = sql.substring(0, sql.length() - 5);
        return sql;
    }

    private String getUpdateSql(Class class_type, Object entity, String schema, String table) throws IllegalAccessException {
        String sql = "update ";
        if(table == null || table.length() == 0){
            throw new IllegalAccessException("name value in @Table annotion is not found.");
        }
        if(schema == null || schema.length() == 0){
            sql += "\"" + table + "\" set ";
        }
        else{
            sql += "\"" + schema + "\".\"" + table + "\" set ";
        }
        
        List<NameValueType> store = new LinkedList<>();
        List<NameValueType> id_field = new LinkedList<>();
        for(Field field : class_type.getDeclaredFields()){
            try {
                if(!field.isAnnotationPresent(Column.class) ){
                    continue;
                }
                boolean nullable = field.getAnnotation(Column.class).nullable();
                Object value = null;
                boolean method_found = false;
                for(Method one : class_type.getMethods() ) {
                    if(one.getName().equals(getGetMethodName(field) ) ){
                        value = one.invoke(entity);
                        method_found = true;
                        break;
                    }
                }
                if(!method_found){
                    continue;
                }
                if(value == null && !nullable){
                    continue;
                }
                if(value == null){
                    continue;
                }
                NameValueType one = new NameValueType();
                String column_name = field.getAnnotation(Column.class).name();
                if(column_name == null || column_name.length() == 0){
                    one.name = field.getName();
                }
                else{
                    one.name = column_name;
                }
                Class type = field.getType();
                if(type.equals(java.util.Date.class) || type.equals(Calendar.class) ){
                    if(field.isAnnotationPresent(Temporal.class) ){
                        TemporalType temporalType = field.getAnnotation(Temporal.class).value();
                        if(temporalType != null) {
                            if(temporalType.equals(TemporalType.DATE) ){
                                if(type.equals(Calendar.class) ){
                                    value = ((Calendar)value).getTime();
                                }
                                type = java.util.Date.class;
                                
                            }
                            else
                            if(temporalType.equals(TemporalType.TIMESTAMP) || temporalType.equals(TemporalType.TIME) ){
                                if(type.equals(java.util.Date.class) ){
                                    Calendar _value = Calendar.getInstance();
                                    _value.setTime((java.util.Date)value);
                                    value = _value;
                                }
                                type = Calendar.class;
                            }
                        }
                    }
                }
                one.value = value;
                one.type = type;
                if(field.isAnnotationPresent(Id.class) ){
                    id_field.add(one);
                }
                else{
                    store.add(one);
                }
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        if(id_field.isEmpty() ){
            throw new IllegalAccessException("@Id annotion is not found.");
        }

        if(store.isEmpty() ){
            return null;
        }
        
        for(NameValueType one : store){
            if(one.name == null || one.name.length() == 0){
                continue;
            }
            sql += "\"" + one.name + "\" = ";
            if(one.value == null){
                sql += "null, ";
            }
            else{
                if(one.type.equals(String.class) ){
                sql += "'" + one.value + "', ";
                }
                else
                if(one.type.equals(java.util.Date.class) ){
                    Calendar cal = Calendar.getInstance();
                    cal.setTime((java.util.Date)one.value);
                    sql += "to_date('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                            + "', '" + date_format + "'), ";
                }
                else
                if(one.type.equals(Calendar.class) ){
                    Calendar cal = (Calendar)one.value;
                    sql += "to_timestamp('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                        + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND) + "', '" + timestamp_format + "'), ";
                }
                else{
                    sql += one.value + ", ";
                }
            }
        }
        sql = sql.substring(0, sql.length() - 2);
        sql += " where ";
        for(NameValueType one : id_field){
            sql += one.name + " = ";
            if(one.type.equals(String.class) ){
                sql += "'" + one.value + "' and ";
            }
            else
            if(one.type.equals(java.util.Date.class) ){
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date)one.value);
                sql += "to_date('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                        + "', '" + date_format + "') and ";
            }
            else
            if(one.type.equals(Calendar.class) ){
                Calendar cal = (Calendar)one.value;
                sql += "to_timestamp('" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DAY_OF_MONTH) 
                    + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" + cal.get(Calendar.MINUTE) + ":" + cal.get(Calendar.SECOND) + "', '" + timestamp_format + "') and ";
            }
            else{
                sql += one.value + " and ";
            }
        }
        sql = sql.substring(0, sql.length() - 5);
        return sql;
    }
}
