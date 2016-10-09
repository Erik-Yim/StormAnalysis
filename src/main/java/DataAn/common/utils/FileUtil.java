package DataAn.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

public class FileUtil {

	/**
	 * 删除文件夹以及其下的文件 或文件夹
	 * 
	 * @param sPath
	 *            被删除目录的文件路径
	 * @return 删除成功返回true
	 * @author ls
	 */
	public static boolean deleteDirectory(String sPath, boolean isDelDir) {
		// 如果sPath不以文件分隔符结尾，自动添加文件分隔符
		if (!sPath.endsWith(File.separator)) {
			sPath = sPath + File.separator;
		}

		File dirFile = new File(sPath);
		// 如果dirFile对应的文件不存在，或者不是一个目录，则退出
		if (!dirFile.exists() || !dirFile.isDirectory()) {
			return false;
		}

		boolean flag = true;
		// 删除文件夹下的所有文件(包括子目录)
		File[] files = dirFile.listFiles();
		for (int i = 0; i < files.length; i++) {
			// 删除子文件
			if (files[i].isFile()) {
				flag = deleteFile(files[i].getAbsolutePath());
				if (!flag)
					break;
			} // 删除子目录
			else {
				flag = deleteDirectory(files[i].getAbsolutePath(),true);
				if (!flag)
					break;
			}
		}
		if (!flag)
			return false;

		if(isDelDir){
			// 删除当前目录
			if (dirFile.delete()) {
				return true;
			} else {
				return false;
			}			
		}
		return true;
	}
	
	
	/**
	 * 删除单个文件
	 * 
	 * @param sPath
	 *            被删除文件的文件名
	 * @return 单个文件删除成功返回true，否则返回false
	 * @author ls
	 */
	public static boolean deleteFile(String sPath) {
		boolean flag = false;
		File file = new File(sPath);
		// 是文件且不为空则进行删除
		if (file.isFile() && file.exists()) {
			file.delete();
			flag = true;
		}
		return flag;
	}
	
	/**
	 * 删除目录下的所有文件
	 * 
	 * @param sPath
	 *            被删除目录的文件路径
	 * @return 目录删除成功返回true，否则返回false
	 * @author ls
	 */
	public static boolean deleteAllFile(String sPath) {
		// 如果sPath不以文件分隔符结尾，自动添加文件分隔符
		if (!sPath.endsWith(File.separator)) {
			sPath = sPath + File.separator;
		}

		File dirFile = new File(sPath);
		// 如果dirFile对应的文件不存在，或者不是一个目录，则退出
		if (!dirFile.exists() || !dirFile.isDirectory()) {
			return false;
		}

		boolean flag = true;
		// 删除文件夹下的所有文件
		File[] files = dirFile.listFiles();
		for (int i = 0; i < files.length; i++) {
			// 删除子文件
			if (files[i].isFile()) {
				flag = deleteFile(files[i].getAbsolutePath());
				if (!flag)
					break;
			}
		}
		if (!flag)
			return false;

		return flag;
	}
	
	public static void saveFile(String dirPath,String fileName,InputStream in) throws Exception{
		File parentDir = new File(dirPath);
		if (!parentDir.exists()) {
			parentDir.mkdirs();
		}
		File file = new File(dirPath,fileName);
		saveFile(file,in);
	}
	public static void saveFile(String filePath,InputStream in) throws Exception{
		File file = new File(filePath);
		saveFile(file,in);
	}
	public static void saveFile(File file,InputStream in) throws Exception{
		//保存csv一个临时文件
		BufferedInputStream bis = null; 
		BufferedOutputStream bos = null;
		try {
			bis = new BufferedInputStream(in);
			bos = new BufferedOutputStream(new FileOutputStream(file));
			int hasRead = 0;  
			byte b[] = new byte[1024];  
			while((hasRead = bis.read(b)) > 0){  
				bos.write(b, 0, hasRead);  
			}  
		} finally{  
            if(bos != null){  
            	bos.flush();  
            	bos.close();    
            }  
            if(bis != null){  
            	bis.close();  
            }  
            if(in != null){
            	in.close();
            }
        }  
	}
	
	/**
	 * 复制目录，仅复制目录下的文件，不复制目录文件夹
	 * 
	 * @param src
	 *            源文件目录
	 * @param des
	 *            目标文件目录
	 * @param create
	 *            是否创建目标文件目录
	 * @return 错误码，0=成功
	 */
	static public int copyDirectory(String src, String des, boolean create) {

		File srcDir = new File(src);
		File dstDir = new File(des);

		if (srcDir.isDirectory()) { //源路径是目录

			if (!dstDir.exists()) { //目的目录不存在

				if (create) {
					dstDir.mkdirs(); // 在目标文件目录创建新的拷贝目录
				} else {
					return 1;
				}
			}
		}

		File[] subFile = srcDir.listFiles();

		for (int i = 0; i < subFile.length; i++) {

			if (subFile[i].isDirectory()) { // 如果文件是目录 遍历

				String dst = new File(dstDir, subFile[i].getName()).getParent();

				copyDirectory(subFile[i].getAbsolutePath(),
						dst + "\\" + subFile[i].getName(), true);

			} else { // 如果是文件 直接上传

				String name = subFile[i].getName();

				InputStream in = null;
				try {
					in = new FileInputStream(subFile[i]);
				} catch (FileNotFoundException e) {

					e.printStackTrace();
				}

				OutputStream out = null;
				try {
					out = new FileOutputStream(new File(dstDir, name));
				} catch (FileNotFoundException e) {

					e.printStackTrace();
				}

				byte[] b = new byte[1024];

				int len;

				try {
					while ((len = in.read(b)) > 0) {
						out.write(b, 0, len);
					}
				} catch (IOException e) {

					e.printStackTrace();
				}

				try {
					in.close();
				} catch (IOException e) {

					e.printStackTrace();
				}
				try {
					out.close();
				} catch (IOException e) {

					e.printStackTrace();
				}
			}
		}

		return 0;
	}
	
	/**
	 * 复制文件
	 * @param srcPath 源文件路径
	 * @param desPath 目标文件路径
	 * @param create true=目录目录
	 * @return
	 */
	static public int copyFile(String srcPath, String desDirectoy, boolean create){
		File srcFile = new File(srcPath);
		assert(srcFile.exists() && srcFile.isFile()); //源文件存在
		
		File desDir = new File(desDirectoy); //目的路径存在
		assert(desDir.isDirectory());
		
		// 创建目录
		if(create && !desDir.exists()){
			desDir.mkdir();
		}
		try {
			//复制文件到目的文件夹
			Files.copy(srcFile.toPath(), desDir.toPath().resolve(srcFile.getName()));
		} catch (Exception e) {
			e.printStackTrace();
			return 1; // 复制文件失败
		}
		
		return 0;
	}

}
