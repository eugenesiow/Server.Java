package org.linkeddatafragments.datasource;

import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import org.linkeddatafragments.exceptions.DataSourceException;
import org.linkeddatafragments.exceptions.UnknownDataSourceTypeException;

/**
 *
 * @author Miel Vander Sande
 */
public class DataSourceFactory {

    public static IDataSource create(JsonObject config) throws DataSourceException {
        String title = config.getAsJsonPrimitive("type").getAsString();
        String description = config.getAsJsonPrimitive("description").getAsString();
        String type = config.getAsJsonPrimitive("type").getAsString();
        
        JsonObject settings = config.getAsJsonObject("settings");

        switch (type) {
            case "HdtDatasource":
                File file = new File(settings.getAsJsonPrimitive("file").getAsString());
                
                try {
                    return new HdtDataSource(title, description, file.getAbsolutePath());
                } catch (IOException ex) {
                    throw new DataSourceException(ex);
                }
            case "HdtMemDatasource":
				File hdtfile = new File(settings.getAsJsonPrimitive("file").getAsString());
                
                try {
                    return new HdtMemDataSource(title, description, hdtfile.getAbsolutePath());
                } catch (IOException ex) {
                    throw new DataSourceException(ex);
                }
            case "SparqlDataSource":
            	return new SparqlDataSource(title,description,settings.getAsJsonPrimitive("endpoint").getAsString());
            case "TdbDatasource":
            	file = new File(settings.getAsJsonPrimitive("file").getAsString());
            	
            	try {
                    return new TdbDataSource(title,description,file.getAbsolutePath());
                } catch (IOException ex) {
                    throw new DataSourceException(ex);
                }
            case "JenaMemDatasource": 
            	File triplesFile = new File(settings.getAsJsonPrimitive("file").getAsString());
            	
            	try {
                    return new JenaMemDataSource(title,description,triplesFile.getAbsolutePath());
                } catch (IOException ex) {
                    throw new DataSourceException(ex);
                }
            default:
                throw new UnknownDataSourceTypeException(type);

        }

    }

}
