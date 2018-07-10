import java.net.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.io.*;
import java.util.*;


public class Storage_Server extends Thread{

	ServerSocket listen22;
	ServerSocket listen;
	//protected static final int  port  = 1333;
	static String linesep = System.getProperty("line.separator");
	
	public static final int buff_size = 1024 * 100;
	byte[] arraybyt2;
	static int count11 = 1;
	//static int blkname11;
	public static final int buff_size1 = 1024 * 100;
	byte[] arraybyt1;
	
	ServerSocketChannel ssc;
	Charset charset1 = Charset.forName("ISO-8859-1");
    CharsetEncoder encoder1 = charset1.newEncoder();
    CharsetDecoder decoder1 = charset1.newDecoder();
	
	public Storage_Server() {
		boolean cdir = (new File("/DHPFS/DFS/StorageB")).mkdir();
		arraybyt2 = new byte[buff_size];
		arraybyt1 = new byte[buff_size1];
	}
	 
	
	public Storage_Server(int port) {
	        try { 
	            listen22 = new ServerSocket(port);
	            System.out.println ("Listening on port: " + listen22.getLocalPort());
	        
	        } catch(Exception e) {System.out.println(e);}
	        
	   }
	/*public Storage_Server(int sport) {
	        try { 
	        	ssc = ServerSocketChannel.open();
				ssc.socket().bind(new InetSocketAddress(sport));
				ssc.configureBlocking(false);
	        } catch(Exception e) {System.out.println(e);}
	        
	   }*/
	
	//Writing blocks from Meta to Storage servers (Distribution)
	   public void BlockDistribution(int Mport)    {
		   String line;
		   StringBuffer sbuf1 = new StringBuffer();
		   
	        try {
	        	listen = new ServerSocket(Mport);
	 		   	System.out.println ("Listening on port: " + listen.getLocalPort());
	 		   	
	            for(;;) {
	                Socket clnt = listen.accept();
	                System.out.println("Server-Client:" +clnt.toString());
	             
	                BufferedReader cin = new BufferedReader(new InputStreamReader(clnt.getInputStream()));
	                String blkname = cin.readLine();
	                System.out.println("Block name--->" + blkname);
	         
	        		//FileWriter cout = new FileWriter("/DHPFS/DFS/StorageB/"+blkname);
	        			
	        		System.out.print("--------------Writing Blocks-------------------");
	        		
	        		BufferedOutputStream cout1 = new BufferedOutputStream (new FileOutputStream("/DHPFS/DFS/StorageB/"+blkname));
					//cin.close();
	        		Socket clnt1 = listen.accept();
	        		BufferedInputStream cin1 = new BufferedInputStream(clnt1.getInputStream());
					  					   
					   int len = 0;
						while ((len = cin1.read(arraybyt1)) > 0) {
							cout1.write(arraybyt1, 0, len);
							
						 }
					
						cin1.close();
						cout1.flush();
						cout1.close();
						clnt.close();
						clnt1.close();
	        		
	        							
	        		System.out.println("\nDone for storage server!");

	            }
	        } catch(Exception e) { System.out.println("'block distr:" + e.getMessage());}
	       
	   }

	 // Read/Write blocks to Client
	 public void ClientReadblocks()    {
	        try {
	            for(;;) {
	            	
	            	// Read block name
	                Socket clnt22 = listen22.accept();
	                System.out.println("Server-Client:" +clnt22.toString());
	                
	                BufferedReader cin1 = new BufferedReader (new InputStreamReader(clnt22.getInputStream()));
	                String blkname = cin1.readLine();
	                System.out.println("Client requesting for Block --->" + blkname);
	                
	                cin1.close();
	                clnt22.close();
	                //TODO:create another object.start(), which starts run()
	                // inside run(), perform I/O as below
	                //create another constructor to initialize blk
	                //Write Blocks
	                
	                File myFile = new File ("/DHPFS/DFS/StorageB/"+blkname);
	                boolean Exist11 = myFile.exists();
	        		if(Exist11) {
	        			System.out.println("\n Requested Block exist in Storage server!!::" + blkname);
	        			Socket clnt33 = listen22.accept();
	                
	        			byte [] mybytearray  = new byte [(int)myFile.length()];
	        			FileInputStream fis = new FileInputStream(myFile);
	        			BufferedInputStream bis = new BufferedInputStream(fis);
	                
	        			bis.read(mybytearray,0,mybytearray.length);
	        			OutputStream os = clnt33.getOutputStream();
	                
	        			System.out.println("Sending Blocks to req Client...");
	        			os.write(mybytearray,0,mybytearray.length);
	               
	        			
	        			fis.close();
	        			bis.close();
	        			os.flush();
	        			os.close();
	        			
	        			clnt33.close();
	        		}else {
	        			System.out.println("\n XX Requested Block does not exist in Storage server XX::" + blkname);
	        		}
	        		
	            }
	        } catch(Exception e) { System.out.println(e); e.printStackTrace();}
	   }

	 public void run() {
		 
		 ClientReadblocks();
		// ReadnewBlocks();
	 }
	 
	 public void ReadnewBlocks(int sport){
		try {
				ServerSocketChannel ssc = ServerSocketChannel.open();
				ssc.socket().bind(new InetSocketAddress(sport));
				//ssc.configureBlocking(false);
				//ssc.blockingLock();
				
				//ByteBuffer cbuf = ByteBuffer.allocate(50);
				while(true){
					
				System.out.println("Storage server waiting for connection...");
				SocketChannel clnt = ssc.accept();
					
				if(clnt == null){
						//wait for client connection
					Thread.sleep(10000);
				}else {
					//m if(count11 == 1){
						ByteBuffer cbuf11 = ByteBuffer.allocate(50);
							
						System.out.println("Incoming connection from: " + clnt.socket().getRemoteSocketAddress());
						clnt.read(cbuf11);
						cbuf11.flip();
						String blkname11 = decoder1.decode(cbuf11).toString(); 
						//to convert byte to string use decoder
						cbuf11.clear();
						System.out.println("Client requesting for block ::" + blkname11);
						count11++;
					//m}
							
				//m}
				clnt.close();
						
				//mwhile(true) {
					
					System.out.println("Storage server waiting for connection22...");
					SocketChannel clnt2 = ssc.accept();
						
					if(clnt2 == null){
					//wait for client connection
					Thread.sleep(10000);
					}
					else {
									
						File myFile = new File ("/DHPFS/DFS/StorageB/"+blkname11);
						//ByteBuffer cbuf = ByteBuffer.allocateDirect(1024);
						boolean Exist11 = myFile.exists();
								
						if(Exist11) {
							System.out.println("\nRequested Block exist in Storage server!!::" + blkname11);
							/*FileInputStream fin = new FileInputStream(myFile);
							FileChannel fc = fin.getChannel();
							MappedByteBuffer cbuf33 = fc.map(MapMode.READ_ONLY, 0, fc.size());*/
									
							/*long flength = myFile.length(); //cor
							
							MappedByteBuffer cbuf33 = new FileInputStream(myFile).getChannel().map(FileChannel.MapMode.READ_ONLY, 0, flength).load();
							System.out.println("Writing blocks..");
							//clnt2.write(cbuf33);
							
							int i = 0;
							while( (cbuf33.hasRemaining()) && (i < flength)){ 
								clnt2.write(cbuf33);
								i++; //cor
							}*/ 
							
							ByteBuffer cbuf22 = ByteBuffer.allocate((int)myFile.length());
							FileInputStream fin = new FileInputStream(myFile);
							FileChannel fc = fin.getChannel();
							fc.read(cbuf22);
							cbuf22.flip();
							while(cbuf22.hasRemaining()){
								clnt2.write(cbuf22);
							}
							
							/*long flength = myFile.length();
							int i = 0;
							while(i < flength){
								clnt2.write(cbuf33);
								i++;
							}*/
								
							//fc.close();
							//fin.close();
							//cbuf33.clear(); //m
																
						}else { System.out.println("\n XX Requested Block does not exist in Storage server XX::" + blkname11);}
									
							
						}
						clnt2.close();
						
					//}
			}//m
		}		
								
			}catch(Exception e){ System.out.println("new sto server Exception:" +e.getMessage());}
					
		}
		
	 
	 public static void main(String argv[]) {
	        
		 if(argv.length != 2) {
			 
			 System.err.println("\nRequired Port and Command-->DistributeBlocks or ReadBlocks");
		 }
		 
		 	int port = Integer.parseInt(argv[0]);
	       // Testserver obj1 = new Testserver(port);
	        //obj1.start();
	      
	        
		    String command = argv[1];
		     if(command.equals("Distribute")){
		    	   Storage_Server obj2 = new Storage_Server();
		    	   obj2.BlockDistribution(port);
		       }
		     
		     if(command.equals("Read-SIO")) {
		    	 Storage_Server obj1 = new Storage_Server(port);
			     //obj1.start();
		    	 //obj2.start();
		    	 obj1.ClientReadblocks();
		       }
		    
		     if(command.equals("Read-NIO")) {
		    	 Storage_Server obj3 = new Storage_Server();
			     //obj1.start();
		    	 //obj2.start();
		    	 obj3.ReadnewBlocks(port);
		    	// obj3.start();
		       }
	         
	    }
}
