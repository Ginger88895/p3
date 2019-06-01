package iiis.systems.os.blockdb;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.Charset;

public class Util
{
    public static JSONObject readJsonFile(String filePath) throws IOException
	{
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        return new JSONObject(content);
    }
	public static String readStringFile(String filePath) throws IOException
	{
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
		return content;
	}
	public static void writeJsonFile(String filePath,JSONObject content) throws IOException
	{
		Files.write(Paths.get(filePath),content.toString().getBytes(Charset.forName("UTF-8")));
	}
	public static void writeStringFile(String filePath,String content) throws IOException
	{
		Files.write(Paths.get(filePath),content.getBytes(Charset.forName("UTF-8")));
	}
	public static void removeFile(String filePath) throws IOException
	{
		Files.delete(Paths.get(filePath));
	}
	public static boolean checkFile(String filePath) throws IOException
	{
		return Files.exists(Paths.get(filePath));
	}
}
