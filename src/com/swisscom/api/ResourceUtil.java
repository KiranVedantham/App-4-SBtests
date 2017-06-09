package com.swisscom.api;


import java.io.File;


public class ResourceUtil {

    public File getFile(String key) throws  Exception {
        
        ClassLoader classLoader = getClass().getClassLoader();
        try{
            File file = new File(classLoader.getResource(key).getFile());
      
           return file;
        } catch (Exception e) {
            String message = "File not found in Application";
            throw new Exception(message);
        }
        
    }


}