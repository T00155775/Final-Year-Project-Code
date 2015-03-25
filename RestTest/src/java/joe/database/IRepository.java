package joe.database;

import org.bson.types.ObjectId;

/*/**
*
* @author Joe O Flaherty
*/
//Repository containing methods that will be implemented for each database
public interface IRepository {
    
    public  String find();

    public String save();

    public String findSingle(ObjectId Id);

}
