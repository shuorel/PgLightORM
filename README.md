# PgLightORM
How can PgLightORM help with PostgreSQL data manipulation?

1. Extend abstract class DbConnectorAbstract to provide database connection configuration:

              public class DbConnector extends DbConnectorAbstract {
              
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
                    
                    //Optional, "5432" by default
                    @Override
                    public String getPort() {
                        return "5432";
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


2. Extend abstract class OrmAbstract to provide connection object:
                
                import java.sql.Connection;
                
                public class Orm extends OrmAbstract {
                    public static Orm getInstance() {
                        return SingletonHolder.INSTANCE;
                    }
            
                    private static class SingletonHolder {
                        private static final Orm INSTANCE = new Orm();
                    }
            
                    private Orm(){}
                    
                    @Override
                    public Connection getConnection() {
                        return DbConnector.getInstance().getConnection();
                    }
                }
                
                
3. Declare entity classes with javax.persistence annotations:

                import javax.persistence.*;
                import java.util.Date;
                import java.util.List;
                
                @Table(schema="stock", name="stock")
                public class StockData {
                    @Id
                    @Column
                    String id;
                    @Column
                    String name;
                    @Column
                    @Temporal(TemporalType.DATE)
                    Date ipo_date;
                    
                    @OneToMany
                    @JoinColumn(name="id", referencedColumnName = "code")
                    List<DividendData> dividend_list;
                
                    public String getId() {
                        return id;
                    }
                
                    public void setId(String id) {
                        this.id = id;
                    }
                
                    public String getName() {
                        return name;
                    }
                
                    public void setName(String name) {
                        this.name = name;
                    }
                
                    public Date getIpo_date() {
                        return ipo_date;
                    }
                
                    public void setIpo_date(Date ipo_date) {
                        this.ipo_date = ipo_date;
                    }
                
                    public List<DividendData> getDividend_list() {
                        return dividend_list;
                    }
                
                    public void setDividend_list(List<DividendData> dividend_list) {
                        this.dividend_list = dividend_list;
                    }
                }
                
                import javax.persistence.*;
                import java.util.Date;

                @Table(schema="stock", name="dividend")
                public class DividendData {
                    @Id
                    @Column
                    String code;
                    @Id
                    @Column
                    @Temporal(TemporalType.DATE)
                    Date date;
                    @Column
                    Double cash;
                    @Column
                    Integer transfer_issue;
                    @Column
                    Integer bonus_issue;
                    @Column
                    @Temporal(TemporalType.DATE)
                    Date register_date;
                    
                    @ManyToOne
                    @JoinColumn(name="code", referencedColumnName = "id")
                    StockData stock;
                
                    public String getCode() {
                        return code;
                    }
                
                    public void setCode(String code) {
                        this.code = code;
                    }
                    
                    public Date getDate() {
                        return date;
                    }
                
                    public void setDate(Date date) {
                        this.date = date;
                    }
                
                    public Date getRegister_date() {
                        return register_date;
                    }
                
                    public void setRegister_date(Date register_date) {
                        this.register_date = register_date;
                    }
                
                    public Integer getBonus_issue() {
                        return bonus_issue;
                    }
                
                    public void setBonus_issue(Integer bonus_issue) {
                        this.bonus_issue = bonus_issue;
                    }
                
                    public Integer getTransfer_issue() {
                        return transfer_issue;
                    }
                
                    public void setTransfer_issue(Integer transfer_issue) {
                        this.transfer_issue = transfer_issue;
                    }
                
                    public Double getCash() {
                        return cash;
                    }
                
                    public void setCash(Double cash) {
                        this.cash = cash;
                    }
                
                    public StockData getStock() {
                        return stock;
                    }
                
                    public void setStock(StockData stock) {
                        this.stock = stock;
                    }
                }
                
                import java.util.List;
                import javax.persistence.*;

                @Table(schema="index", name="index")
                public class IndexData {
                    @Id
                    @Column
                    private String id;
                    
                    @OneToMany
                    @JoinColumn(name="id", referencedColumnName = "index")
                    List<StockIndex> stock_index_list;
                
                    public String getId() {
                        return id;
                    }
                
                    public void setId(String id) {
                        this.id = id;
                    }
                
                    public List<StockIndex> getStock_index_list() {
                        return stock_index_list;
                    }
                
                    public void setStock_index_list(List<StockIndex> stock_index_list) {
                        this.stock_index_list = stock_index_list;
                    }
                }
                
                import javax.persistence.*;
                import java.util.Date;

                @Table(schema="index", name="stock_list")
                public class StockIndex {
                    @Id
                    @Column
                    String index;
                    @Id
                    @Column
                    String stock_id;
                    @Id
                    @Column
                    @Temporal(TemporalType.DATE)
                    Date added_date;
                    @Column
                    String name;
                    @Column
                    @Temporal(TemporalType.DATE)
                    Date removed_date;
                    
                    @ManyToOne
                    @JoinColumn(name="index", referencedColumnName = "id")
                    IndexData index_data;
                    
                    @ManyToOne
                    @JoinColumn(name="stock_id", referencedColumnName = "id")
                    StockData stock;
                
                    public String getIndex() {
                        return index;
                    }
                
                    public void setIndex(String index) {
                        this.index = index;
                    }
                
                    public String getStock_id() {
                        return stock_id;
                    }
                
                    public void setStock_id(String stock_id) {
                        this.stock_id = stock_id;
                    }
                
                    public String getName() {
                        return name;
                    }
                
                    public void setName(String name) {
                        this.name = name;
                    }
                
                    public Date getAdded_date() {
                        return added_date;
                    }
                
                    public void setAdded_date(Date added_date) {
                        this.added_date = added_date;
                    }
                
                    public Date getRemoved_date() {
                        return removed_date;
                    }
                
                    public void setRemoved_date(Date removed_date) {
                        this.removed_date = removed_date;
                    }
                
                    public IndexData getIndex_data() {
                        return index_data;
                    }
                
                    public void setIndex_data(IndexData index_data) {
                        this.index_data = index_data;
                    }
                
                    public StockData getStock() {
                        return stock;
                    }
                
                    public void setStock(StockData stock) {
                        this.stock = stock;
                    }
                }
     
                
4. Basic operations:

    Insert a new IndexData record
    
            Orm orm = Orm.getInstance();
            IndexData data = new IndexData();
            data.setId("sh000010");
            orm.insert(data);
            
            
    Update a StockData record
    
            Orm orm = Orm.getInstance();
            StockData data = new StockData();
            data.setId("600000");
            data.setName("浦发银行");
            data.setIpo_date(null);
            orm.update(data);
            
            
    Update a StockData record, or insert a new record if not exist:
    
            Orm orm = Orm.getInstance();
            StockData data = new StockData();
            data.setId("600000");
            data.setName("浦发银行");
            data.setIpo_date(null);
            orm.upsert(data);
            
            
    Get DividendData list associated with a StockData record(OneToMany association):
    
            Orm orm = Orm.getInstance();
            StockData data = new StockData();
            data.setId("600000");
            orm.associate(data, DividendData.class);
            for(DividendData one : data.getDividend_list() ) {
                System.out.println(Util.dateToString(one.getDate() ) + ": " + one.getCash() );
            }
            
            
    Get StockData record associated with a DividendData record(OneToOne or ManyToOne association):
    
            Orm orm = Orm.getInstance();
            DividendData data = new DividendData();
            data.setCode("600000");
            orm.associate(data, StockData.class);
            System.out.println("got: " + data.getStock().getName() );
            
            
    Get StockData list associated with a IndexData record(ManyToMany association with intermediate table):
    
            Orm orm = Orm.getInstance();
            IndexData data = new IndexData();
            data.setId("sh000016");
            orm.associate(data, StockIndex.class);
            for(StockIndex one : data.getStock_index_list() ) {
                orm.associate(one, StockData.class);
                System.out.println(one.getStock().getName() );
            }
      
      
    Fill in an entity object with single select result:
      
            StockData data = new StockData();
            Orm orm = Orm.getInstance();
            String query = "select * from stock.stock where id = '600000'";
            try(PreparedStatement stmt = orm.getConnection().prepareStatement(query) ) {
                ResultSet rs = stmt.executeQuery();
                if(orm.fillEntity(data, rs) ) {
                    System.out.println("StockData: " + data.getName() );
                }
            } catch (SQLException e) {
                Logger.getAnonymousLogger().log(Level.SEVERE, null, e);
            }
            
            
      Fill in an entity list with select results:
      
            List<StockData> data = new LinkedList<>();
            Orm orm = Orm.getInstance();
            String query = "select * from stock.stock order by id";
            try(PreparedStatement stmt = orm.getConnection().prepareStatement(query) ) {
                ResultSet rs = stmt.executeQuery();
                if(orm.fillEntityList(StockData.class, data, rs) ) {
                    for(StockData one : data) {
                        System.out.println("StockData: " + one.getName() );
                    }
                }
            } catch (SQLException e) {
                Logger.getAnonymousLogger().log(Level.SEVERE, null, e);
            }
