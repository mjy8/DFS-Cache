
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.Socket;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;


public class Blkcache {

	public static Map<String, Object> mblk = new HashMap<String, Object>(); //(blk, readBLK())
	public static LinkedList<String> Mrublk = new LinkedList<String>(); //List of blks in cache
	Map<String, Long> time = new HashMap<String, Long>();
	
	int cachesize = 100; //no objects kept in memory
	static String linesep = System.getProperty("line.separator");
	public ExecutorService threadd = Executors.newFixedThreadPool(100);
	public static final int buff_size = 1024 * 100;
	byte[] arraybyt = new byte[buff_size];
	byte [] bytearray;
	ByteBuffer cbuf22;
	Charset charset1 = Charset.forName("ISO-8859-1");
    CharsetEncoder encoder1 = charset1.newEncoder();
    CharsetDecoder decoder1 = charset1.newDecoder();
	
	/*public Runnable removec(final String name){
		return new Runnable() {
			public void run() {
				synchronized (this) {
					mblk.remove(name);
					time.remove(name);
				}
			}
		};
	}*/
	
	// Create tuple<hashmap> and Put Blocks in memory
	public void PutBlk(String blkname) {
		
		//if Map doesn't contain key and cache is full
		if(!(mblk.containsKey(blkname))) {
			
			synchronized(this){
			try{	
				if((Mrublk.size()) >= cachesize){
					System.out.println("\n\n %%%%%%%% BLK removed from list %%%%%%%%%%%");
					//TODO:Also measure cache size of Hash-map instead of linked_list 
					mblk.remove(Mrublk.getLast());
					Mrublk.removeLast();
					//blks are removed - update Meta server
					System.out.println("\n Update Meta server C-table.....");
					Update_Meta();
					//Thread.sleep(5000);
				}
			}catch(Exception e){System.out.println("\n Blk cache sleep..."+ e.getMessage());}
				mblk.put(blkname, ReadBlkHP(blkname));
				Mrublk.addFirst(blkname);
			}
		}else {
			synchronized(this) {
				if((Mrublk.size()) >= cachesize){
					System.out.println("%%%%%%%% BLK removed from list %%%%%%%%%%%");
					mblk.remove(Mrublk.getLast());
					Mrublk.removeLast();
					//blks are removed - update Meta server
					System.out.println("\n Update Meta server C-table.....");
					Update_Meta();
				}
			
				mblk.remove(blkname);
				Mrublk.remove(blkname);
				mblk.put(blkname, ReadBlkHP(blkname));
				Mrublk.addFirst(blkname);
			}
		}
		//return (mblk.get(blkname).toString());
		//return mblk.get(blkname);
	}
	public void Update_Meta() {
		
		Client_Service objc = new Client_Service();
		try {
			
			//Update Meta server or C-table
			Socket clntblk = new Socket(objc.hostname, objc.mportu);
			//PrintWriter mout22 = new PrintWriter(clntU.getOutputStream (),true);
			System.out.println("\nEstablishing connection with Meta server to update C-table on port: " + objc.mportu);
			
			ObjectOutputStream objblk = new ObjectOutputStream(clntblk.getOutputStream());
			System.out.println("\n\n Blk expired/updating meta ..Mapblks and Mrublk list:::" + Mrublk + mblk );
			//Writing blkname linked list and mapped blkname->blkid,host/port
			objblk.writeObject(Mrublk);
			objblk.writeObject(mblk);
			
			clntblk.close();
			objblk.close();
			
		}catch(Exception e){ System.out.println("Blk cache Exception:" +e.getMessage());}
		
		
	}
	//Get Blocks from memory
	public byte[] GetBlk(String blkname) {
	
		byte[] blkdata = null;
		try {
			if(!(Mrublk.contains(blkname) && mblk.containsKey(blkname))){
				System.out.println("\n Block does not exist in cache XX");
				return null;
			}
			else {
				//converting object into byte array[]
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(mblk.get(blkname));
				oos.flush();
				oos.close();
				bos.close();
				blkdata = bos.toByteArray();
				return blkdata;
			}
		
		}catch(Exception e) { System.out.println("return cache blk exception:" + e.getMessage());}
	
		return blkdata;
	}
		
	
	/*
	public String ReadBlk(String blkname) {
		
		StringBuffer buff = new StringBuffer();
		String blkline = null;
	
		try{	
			File fn = new File("/DHPFS/DFS/StorageB/"+ blkname);
			//StringBuffer buff = new StringBuffer();
			BufferedReader in = new BufferedReader(new FileReader(fn));
			
			while((blkline = in.readLine()) != null) {
				
				buff.append(blkline);
				buff.append(linesep);
			}
			//System.out.println("buff size :" + buff.capacity());
			//return buff.toString();
			in.close();
		}catch(Exception e) { System.out.println("file exception:" + e.getMessage());}
		//System.out.println(buff.toString());
		
		return buff.toString();
	}*/
	
	public byte[] ReadBlkHP(String blkname) {
		
		try{	
			File fn = new File("/DHPFS/Client/"+blkname);
			bytearray  = new byte [(int)fn.length()];
			FileInputStream fis = new FileInputStream(fn);
			BufferedInputStream bis = new BufferedInputStream(fis);
        
			bis.read(bytearray,0,bytearray.length);
			
			/*int offset = 0;
	        int numRead = 0;
	        while (offset < bytearray.length
	               && (numRead=bis.read(bytearray, offset, bytearray.length-offset)) >= 0) {
	            offset += numRead;
	        }*/
			bis.close();
			
		}catch(Exception e) { System.out.println("file exception:" + e.getMessage());}
		String ss = new String(bytearray);
		//System.out.println(ss);
	    /*for (int i=0; i < bytearray.length; i++) {
	        System.out.print((char)bytearray[i]);
	    }*/
		return bytearray;
	}

	public String ReadBlkMem(String blkname) {
		
		String cblk = null;
		try{	
			File fn = new File("/DHPFS/Client/"+blkname);
			
			cbuf22 = ByteBuffer.allocateDirect((int)fn.length());
			FileInputStream fin = new FileInputStream(fn);
			FileChannel fc = fin.getChannel();
			fc.read(cbuf22);
			cbuf22.flip();
			
			cblk = decoder1.decode(cbuf22).toString();
				
		}catch(Exception e) { System.out.println("file exception:" + e.getMessage());}
		return cblk;
		//return cbuf22;
	}
	
	
	public static void main(String args[]) {
		
		/*Blkcache obj = new Blkcache();
		obj.PutBlk("Blk0.sim");
		obj.PutBlk("LFile.sim1");
		System.out.println("List of blocks in cache---1:" + Mrublk);*/
		
		/*try {
			Thread.sleep(1000);
		}catch(Exception e){ System.out.println("sleep:" + e);}*/
		
		//Print the jvm heap size.
		long heapSize = Runtime.getRuntime().totalMemory();
	    System.out.println("Heap Size = " + heapSize);
	    // java -Xms64m -Xmx256m to increase heap size
		 //Max heap space approx 16 MB (for blocks)
		
	    
	   	System.out.println("List of blocks in cache:" + Mrublk);
				
		
		/*
		try {
			Thread.sleep(10000);
		}catch(Exception e){ System.out.println("sleep:" + e);}
		*/
		System.out.println("\n************************************************************************\n");
		//System.out.println("Result::" + obj.GetBlk("Blk0.sim"));
	}
	
}
