package ceng.ceng351.foodrecdb;

import java.sql.*;

public class FOODRECDB implements IFOODRECDB {

    Connection connection = null;
    /**
     * Place your initialization code inside if required.
     *
     * <p>
     * This function will be called before all other operations. If your implementation
     * need initialization , necessary operations should be done inside this function. For
     * example, you can set your connection to the database server inside this function.
     */
    @Override
    public void initialize(){
        String user = "e2448512"; // TODO: Your userName
        String password = "lxi6s8daAYbnA3UA"; //  TODO: Your password
        String host = "momcorp.ceng.metu.edu.tr"; // host name
        String database = "db2448512"; // TODO: Your database name
        int port = 8080; // port

        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }

    /**
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    @Override
    public int createTables(){
        String query1 = "CREATE TABLE IF NOT EXISTS MenuItems(" +
                        "itemID INTEGER PRIMARY KEY," +
                        "itemName VARCHAR(40)," +
                        "cuisine VARCHAR(20)," +
                        "price INTEGER" +
                        ")";
        String query2 = "CREATE TABLE IF NOT EXISTS Ingredients(" +
                        "ingredientID INTEGER PRIMARY KEY," +
                        "ingredientName VARCHAR(40)" +
                        ")";
        String query3 = "CREATE TABLE  IF NOT EXISTS Includes(" +
                        "itemID INTEGER," +
                        "ingredientID INTEGER," +
                        "PRIMARY KEY(itemID,ingredientID)," +
                        "FOREIGN KEY(itemID) REFERENCES MenuItems(itemID)," +
                        "FOREIGN KEY(ingredientID) REFERENCES Ingredients(ingredientID)" +
                        ")";
        String query4 = "CREATE TABLE IF NOT EXISTS Ratings(" +
                        "ratingID INTEGER PRIMARY KEY," +
                        "itemID INTEGER," +
                        "rating INTEGER," +
                        "ratingDate DATE," +
                        "FOREIGN KEY(itemID) REFERENCES MenuItems(itemID)" +
                        ")";
        String query5 = "CREATE TABLE IF NOT EXISTS DietaryCategories(" +
                        "ingredientID INTEGER," +
                        "dietaryCategory VARCHAR(20)," +
                        "PRIMARY KEY(ingredientID,dietaryCategory)," +
                        "FOREIGN KEY(ingredientID) REFERENCES Ingredients(ingredientID)" +
                        ")";
        try{
            PreparedStatement create_user = connection.prepareStatement(query1);
            PreparedStatement create_vaccine = connection.prepareStatement(query2);
            PreparedStatement create_vaccination = connection.prepareStatement(query3);
            PreparedStatement create_alergy = connection.prepareStatement(query4);
            PreparedStatement create_seen = connection.prepareStatement(query5);

            create_user.executeUpdate();
            create_vaccine.executeUpdate();
            create_vaccination.executeUpdate();
            create_alergy.executeUpdate();
            create_seen.executeUpdate();
        } catch (Exception e){
            System.out.println(e);
        }

        return 5;
    }

    /**
     * Should drop the tables if exists when called.
     *
     * @return the number of tables are dropped successfully.
     */
    @Override
    public int dropTables(){
        String drop_command1 = "DROP TABLE MenuItems;";
        String drop_command2 = "DROP TABLE Ingredients;";
        String drop_command3 = "DROP TABLE Includes;";
        String drop_command4 = "DROP TABLE Ratings;";
        String drop_command5 = "DROP TABLE DietaryCategories;";
        try{
            PreparedStatement drop_tables1 = connection.prepareStatement(drop_command1);
            PreparedStatement drop_tables2 = connection.prepareStatement(drop_command2);
            PreparedStatement drop_tables3 = connection.prepareStatement(drop_command3);
            PreparedStatement drop_tables4 = connection.prepareStatement(drop_command4);
            PreparedStatement drop_tables5 = connection.prepareStatement(drop_command5);
            drop_tables3.executeUpdate();
            drop_tables4.executeUpdate();
            drop_tables5.executeUpdate();
            drop_tables1.executeUpdate();
            drop_tables2.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
            return 0;
        }

        return 5;
    }

    /**
     * Should insert an array of MenuItem into the database.
     *
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertMenuItems(MenuItem[] items){
        int count = 0;
        PreparedStatement ps = null;
        for (MenuItem m: items){
            try{
                String query = "INSERT INTO MenuItems(itemID,itemName,cuisine,price) " +
                        "values(?,?,?,?)";
                ps = connection.prepareStatement(query);
                ps.setInt(1, m.getItemID());
                ps.setString(2, m.getItemName());
                ps.setString(3,m.getCuisine());
                ps.setInt(4,m.getPrice());
                ps.executeUpdate();
                ps.close();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    /**
     * Should insert an array of Ingredient into the database.
     *
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertIngredients(Ingredient[] ingredients){
        int count = 0;
        PreparedStatement ps = null;
        for (Ingredient i: ingredients){
            try{
                String query = "INSERT INTO Ingredients(ingredientID,ingredientName) " +
                        "values(?,?)";
                ps = connection.prepareStatement(query);
                ps.setInt(1, i.getIngredientID());
                ps.setString(2, i.getIngredientName());
                ps.executeUpdate();
                ps.close();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    /**
     * Should insert an array of Includes into the database.
     *
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertIncludes(Includes[] includes){
        int count = 0;
        PreparedStatement ps = null;
        for (Includes i: includes){
            try{
                String query = "INSERT INTO Includes(itemID,ingredientID) " +
                        "values(?,?)";
                ps = connection.prepareStatement(query);
                ps.setInt(1, i.getItemID());
                ps.setInt(2, i.getIngredientID());
                ps.executeUpdate();
                ps.close();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    /**
     * Should insert an array of DietaryCategory into the database.
     *
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertDietaryCategories(DietaryCategory[] categories){
        int count = 0;
        PreparedStatement ps = null;
        for (DietaryCategory c: categories){
            try{
                String query = "INSERT INTO DietaryCategories(ingredientID, dietaryCategory) " +
                        "values(?,?)";
                ps = connection.prepareStatement(query);
                ps.setInt(1, c.getIngredientID());
                ps.setString(2, c.getDietaryCategory());
                ps.executeUpdate();
                ps.close();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    /**
     * Should insert an array of Seen into the database.
     *
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertRatings(Rating[] ratings){
        int count = 0;
        PreparedStatement ps = null;
        for (Rating r: ratings){
            try{
                String query = "INSERT INTO Ratings(ratingID, itemID, rating, ratingDate) " +
                        "values(?,?,?,?)";
                ps = connection.prepareStatement(query);
                ps.setInt(1, r.getRatingID());
                ps.setInt(2, r.getItemID());
                ps.setInt(3, r.getRating());
                ps.setString(4, r.getRatingDate());
                ps.executeUpdate();
                ps.close();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    /**
     * Should return menu items that include the given ingredient
     *
     * @return MenuItem[]
     */
    @Override
    public MenuItem[] getMenuItemsWithGivenIngredient(String name){
        String query =  "SELECT DISTINCT M.itemID, M.itemName, M.cuisine, M.price " +
                        "FROM MenuItems M, Includes I, Ingredients S " +
                        "WHERE S.ingredientName= '" + name + "'  AND M.itemID=I.itemID AND I.ingredientID=S.ingredientID "+
                        "ORDER BY M.itemID ASC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        MenuItem[] res = new MenuItem[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");

                MenuItem m = new MenuItem(itemID, itemName, cuisine, price);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }

    /**
     * Should return menu items that do not have any ingredients in the Includes table
     *
     * @return MenuItem[]
     */
    @Override
    public MenuItem[] getMenuItemsWithoutAnyIngredient(){
        String query =  "SELECT DISTINCT * " +
                        "FROM MenuItems M " +
                        "WHERE M.itemID NOT IN (SELECT DISTINCT I.itemID FROM Includes I) "+
                        "ORDER BY M.itemID ASC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        MenuItem[] res = new MenuItem[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");

                MenuItem m = new MenuItem(itemID, itemName, cuisine, price);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }


    /**
     * Should return ingredients that are not included in any menu item
     *
     * @return Ingredient[]
     */
    @Override
    public Ingredient[] getNotIncludedIngredients(){
        String query =  "SELECT DISTINCT * " +
                        "FROM Ingredients I " +
                        "WHERE I.ingredientID NOT IN (SELECT DISTINCT Inc.ingredientID FROM Includes Inc) "+
                        "ORDER BY I.ingredientID ASC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        Ingredient[] res = new Ingredient[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                int ingredientID = rs.getInt("ingredientID");
                String ingredientName = rs.getString("ingredientName");
                Ingredient m = new Ingredient(ingredientID,ingredientName);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }


    /**
     * Should return menu item with the highest different ingredient number
     *
     * @return MenuItem
     */
    @Override
    public MenuItem getMenuItemWithMostIngredients(){
        String query =  "SELECT M.itemID, M.itemName, M.cuisine, M.price " +
                        "FROM MenuItems M," +
                            "(SELECT I.itemID, COUNT(DISTINCT I.ingredientID) " +
                              "FROM Includes I " +
                              "GROUP BY I.itemID " +
                              "HAVING COUNT(DISTINCT I.ingredientID) >= " +
                                             "ALL(SELECT COUNT(DISTINCT I2.ingredientID) " +
                                                 "FROM Includes I2 "  +
                                                 "GROUP BY I2.itemID" +
                                                 ")" +
                            ") T1 " +
                        "WHERE M.itemID = T1.itemID";

        MenuItem m = null;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            rs.next();
            int itemID = rs.getInt("itemID");
            String itemName = rs.getString("itemName");
            String cuisine = rs.getString("cuisine");
            int price = rs.getInt("price");
            m = new MenuItem(itemID, itemName, cuisine, price);
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return m;
    }


    /**
     * Should return menu items with their ID, name, and average rating
     *
     * @return MenuItemAverageRatingResult[]
     */
    @Override
    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings(){
        String query =  "SELECT DISTINCT M.itemID, M.itemName, AVG(R.rating) as avgRating " +
                        "FROM MenuItems M NATURAL LEFT OUTER JOIN Ratings R " +
                        "GROUP BY M.itemID, M.itemName " +
                        "ORDER BY avgRating DESC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        QueryResult.MenuItemAverageRatingResult[] res = new QueryResult.MenuItemAverageRatingResult[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                String itemID = rs.getString("itemID");
                String itemName = rs.getString("itemName");
                String avgRating = rs.getString("avgRating");

                QueryResult.MenuItemAverageRatingResult m = new QueryResult.MenuItemAverageRatingResult(""+itemID,itemName,""+avgRating);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }

    /**
     * Should return menu items that are suitable for a given dietary category
     *
     * @return MenuItem[]
     */
    @Override
    public MenuItem[] getMenuItemsForDietaryCategory(String category){
        String query =  "SELECT DISTINCT * " +
                        "FROM MenuItems M " +
                        "WHERE M.itemID NOT IN( " +
                                "SELECT I.itemID " +
                                "FROM Includes I " +
                                "WHERE NOT EXISTS(SELECT * " +
                                              "FROM DietaryCategories D " +
                                               "WHERE D.dietaryCategory= '" + category + "' AND I.ingredientID=D.ingredientID) "+
                                ") AND M.itemID IN(SELECT I.itemID " +
                                                  "FROM Includes I)" +
                        "ORDER BY M.itemID ASC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        MenuItem[] res = new MenuItem[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");

                MenuItem m = new MenuItem(itemID, itemName, cuisine, price);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }

    /**
     * Should return the most used ingredient across all menu items
     *
     * @return Ingredient
     */
    @Override
    public Ingredient getMostUsedIngredient(){
        String query =  "SELECT * " +
                        "FROM Ingredients I," +
                        "(SELECT INC.ingredientID, COUNT(DISTINCT INC.itemID) " +
                         "FROM Includes INC " +
                         "GROUP BY INC.ingredientID " +
                         "HAVING COUNT(DISTINCT INC.itemID) >= " +
                                    "ALL(SELECT COUNT(DISTINCT I2.itemID) " +
                                        "FROM Includes I2 "  +
                                        "GROUP BY I2.ingredientID" +
                                        ")" +
                        ") T1 " +
                        "WHERE I.ingredientID = T1.ingredientID";

        Ingredient i = null;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            rs.next();
            int ingredientID = rs.getInt("ingredientID");
            String ingredientName = rs.getString("ingredientName");
            i = new Ingredient(ingredientID,ingredientName);
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return i;
    }


    /**
     * Should return cuisine names with their average ratings
     *
     * @return CuisineWithAverageResult[]
     */
    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating(){
        String query =  "SELECT DISTINCT M.cuisine, AVG(R.rating) as averageRating " +
                        "FROM MenuItems M NATURAL LEFT OUTER JOIN Ratings R " +
                        "GROUP BY M.cuisine " +
                        "ORDER BY averageRating DESC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        QueryResult.CuisineWithAverageResult[] res = new QueryResult.CuisineWithAverageResult[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                String cuisine = rs.getString("cuisine");
                String avgRating = rs.getString("averageRating");

                QueryResult.CuisineWithAverageResult m = new QueryResult.CuisineWithAverageResult(cuisine,avgRating);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }


    /**
     * Should return cuisine names with their average ingredient count per item
     *
     * @return CuisineWithAverageResult[]
     */
    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount(){
        String query =  "SELECT DISTINCT T.cuisine, AVG(T.cnt) as averageCount " +
                        "FROM (SELECT M.cuisine, M.itemID, COUNT(I.ingredientID) as cnt " +
                                           "FROM MenuItems M NATURAL LEFT OUTER JOIN Includes I " +
                                           "GROUP BY M.cuisine, M.itemID) T " +
                        "GROUP BY T.cuisine " +
                        "ORDER BY averageCount DESC";

        int resultSize = 0;

        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        QueryResult.CuisineWithAverageResult[] res = new QueryResult.CuisineWithAverageResult[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                String cuisine = rs.getString("cuisine");
                String averageCount = rs.getString("averageCount");

                QueryResult.CuisineWithAverageResult m = new QueryResult.CuisineWithAverageResult(cuisine,averageCount);
                res[i] = m;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }


    /**
     * Should increase the price of all menu items that include the given ingredient by the given amount
     *
     * @return the row count for SQL Data Manipulation Language (DML) statements
     */
    @Override
    public int increasePrice(String ingredientName, String increaseAmount){
        String query =  "UPDATE MenuItems M SET M.price = (M.price+'"+increaseAmount+"')" +
                        "WHERE M.itemID IN(SELECT INC.itemID FROM Includes INC, Ingredients I " +
                                          "WHERE INC.ingredientID=I.ingredientID AND I.ingredientName= '" + ingredientName + "')";

        int rs=0;
        try{
            Statement connectionStatement = connection.createStatement();
            rs = connectionStatement.executeUpdate(query);
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return rs;
    }

    /**
     * Should delete and return ratings that have an earlier rating date than the given date
     *
     * @return Rating[]
     */
    @Override
    public Rating[] deleteOlderRatings(String date){
        String deleteQuery =  "DELETE FROM Ratings R " +
                              "WHERE R.ratingDate<'" + date + "'";
        String query =  "SELECT * FROM Ratings R " +
                        "WHERE R.ratingDate<'" + date + "';";

        int resultSize = 0;
        try{
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery(query);
            while (rs.next()) {
                resultSize++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        Rating[] res = new Rating[resultSize];
        int i = 0;
        try{
            Statement connectionStatement = connection.createStatement();
            ResultSet rs = connectionStatement.executeQuery(query);
            while (rs.next()){
                int ratingID = rs.getInt("ratingID");
                int itemID = rs.getInt("itemID");
                int rating = rs.getInt("rating");
                String ratingDate = rs.getString("ratingDate");

                Rating r = new Rating(ratingID,itemID,rating,ratingDate);
                res[i] = r;
                i++;
            }
            rs.close();
        } catch (Exception e)
        {
            System.out.println(e);
        }

        try{
            Statement connectionStatement = connection.createStatement();
            connectionStatement.executeUpdate(deleteQuery);
        } catch (Exception e)
        {
            System.out.println(e);
        }
        return res;
    }

}
