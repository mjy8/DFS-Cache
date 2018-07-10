import java.net.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.io.*;
import java.util.*;


public class Client extends Thread implements ReadIO{

	
	//static int Storage_Servers_No = 8;//y
	BufferedOutputStream cout1 = null;
	BufferedInputStream cin1 = null;
	public static final int buff_size = 1024 * 100;//100 KB
	byte[] arraybyt1;
	String hostname = "localhost";
    //int    port = 1444;
    int mport = 8888;
    static HashMap<String,ArrayList<Object>> bport1; // <blkid-->host,port>
	
    Charset charset = Charset.forName("ISO-8859-1");
    CharsetEncoder encoder = charset.newEncoder();
    CharsetDecoder decoder = charset.newDecoder();
	
    public Client() {
		boolean cdir = (new File("/DHPFS/Client")).mkdir();
		arraybyt1 = new byte[buff_size];
	}
	
   
	
	 // Connect to Meta Server to get block location
	 public void ConnectToMetaserver(String filename)       {
		   try {
				//InetAddress hostip  = InetAddress.getByName(hostname);
				//Creating Client Socket
				Socket clnt = new Socket(hostname, mport);
				PrintWriter mout = new PrintWriter(clnt.getOutputStream (),true);
				//Cannot perform read and write both on socket simulty
				
				System.out.println("Establishing connection with Meta server on port: " + mport);
				System.out.println("Client:" + clnt);
				
				//Write to socket
				System.out.println("\n------------------Getting Block location from Meta Server-------------------->");
				System.out.println("Client getMetadata() from Meta-Server :-->" + filename);
				mout.write(filename);
								
				mout.close();
				//clnt.close();
				
				/*Socket clnt1 = new Socket(hostname, mport);
				BufferedReader iread = new BufferedReader(new InputStreamReader(clnt1.getInputStream()));
				String line = iread.readLine();
				System.out.println("Written from master server::-->" +line);
				iread.close();
				//clnt1.close(); */
				
				//get Meta data (File, block, host/port)
				Socket clnt1 = new Socket(hostname, mport);
				ObjectInputStream ooi = new ObjectInputStream(clnt1.getInputStream());
				//<filename1 -> blkid1,2,..n>
				Map<String, ArrayList<String>>  btable = (Map) ooi.readObject();
				@SuppressWarnings("unchecked")
				Map<String, ArrayList<String>> btable1 = new HashMap<String,ArrayList<String>>(btable);
				System.out.println("\nMetadata blk list read from Meta Server soc::" + btable1);
				//<blkid1->host,port>..blkid_n
				HashMap<String,ArrayList<Object>> bport = (HashMap) ooi.readObject();
				bport1 = new HashMap<String,ArrayList<Object>>(bport);
				System.out.println("\nMetadata host/port list from Meta Server soc::" + bport1);
							
				ooi.close();
				clnt.close();
				clnt1.close();//Closing the Meta server soc XX
				System.out.println("\n--------------Metadata() from Meta server !!------------> ");
				
				/*for (Map.Entry<Integer, ArrayList<Object>> e : bport1.entrySet()){
					
					int sblkname = e.getKey();
					String shost = e.getValue().get(0).toString();
					int sport = Integer.parseInt(e.getValue().get(1).toString());
					
				    //System.out.println("gettt-->"+ e.getKey() + ": " + e.getValue().get(0).toString()+ "::" + e.getValue().get(1));
				   
					ConnectToStorageserver(sblkname, shost, sport);
				 }*/
			
			}catch(Exception e){ System.out.println("Exception:" +e.getMessage());}
	   }
	 
	 public void ConnectToCooperativecache(String blkname, String host, int port) {
		 String Shost = host;
		 int Sport = port;
			try {
					
					// Writing Block name 
					Socket sclnt2 = new Socket(Shost, Sport);
				
					System.out.println("\nEstablishing connection with Coperativecache on port: " + port );
					System.out.println("\nGet block from Storage/Cooperative ::" + blkname);
				
					PrintWriter sout = new PrintWriter(sclnt2.getOutputStream());
					sout.write(blkname);	   
					   
					sout.close();
								
				
					// Read Blocks
					Socket sclnt = new Socket(Shost, Sport);
					System.out.println("\nEstablishing connection with Cooperative on port: " + sclnt.getLocalPort()+"-->"+ Sport);
					System.out.println("Client:" + sclnt);
					
					   //Read from socket //to support cache updating blkname to - blkid(blkname+storserver port)
					   cout1 = new BufferedOutputStream (new FileOutputStream("/DHPFS/Client/"+blkname+Sport));
					   ObjectInputStream cino = new ObjectInputStream(sclnt.getInputStream());
					   System.out.println("\nClient reading blocks from Storage/Cooperative socket:-->" + blkname );
					   
					  ObjectOutputStream cobj = new ObjectOutputStream(cout1);
					  cobj.writeObject(cino.readObject());
					   
					  
					  	cobj.flush();
					  	cobj.close();
						cin1.close();
						cout1.flush();
						cout1.close();
						sclnt2.close();
						sclnt.close();
						
				
				}catch(Exception e){ System.out.println("Exception:" +e.getMessage());}
		 }
	 
	 //Connect to Storage server to get actual blocks(READ) 
		public void  ConnectToStorageserverSIO(String blkname, String host, int port)       {
			String Shost = host;
			int Sport = port;
			try {
					
					// Writing Block name 
					Socket sclnt2 = new Socket(Shost, Sport);
				
					System.out.println("\nEstablishing connection with storage/Coperative on port: " + port );
					System.out.println("\nGet block from Storage/Cooperative ::" + blkname);
				
					PrintWriter sout = new PrintWriter(sclnt2.getOutputStream());
					sout.write(blkname);	   
					   
					sout.close();
								
				
					// Read Blocks
					Socket sclnt = new Socket(Shost, Sport);
					System.out.println("\nEstablishing connection with Storage/Cooperative on port: " + sclnt.getLocalPort()+"-->"+ Sport);
					System.out.println("Client:" + sclnt);
					
					   //Read from socket //to support cache updating blkname to - blkid(blkname+storserver port)
					   cout1 = new BufferedOutputStream (new FileOutputStream("/DHPFS/Client/"+blkname+Sport));
					   cin1 = new BufferedInputStream(sclnt.getInputStream());
					   System.out.println("\nClient reading blocks from Storage/Cooperative socket:-->" + blkname );
					   
					   
					   
					   int len = 0;
						while ((len = cin1.read(arraybyt1)) > 0) {
							cout1.write(arraybyt1, 0, len);
							
						 }
					
						cin1.close();
						cout1.flush();
						cout1.close();
						sclnt2.close();
						sclnt.close();
				
				}catch(Exception e){ System.out.println("Exception:" +e.getMessage());}
		   }
		
	
		public void ConnectToStorageserverNIO(String blk, String hostn, int port) {
			
			try {
					byte[] blkbyte = blk.getBytes();
					InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
					SocketChannel sc = SocketChannel.open();
					
					System.out.println("\nEstablishing connection with Storage server on port: " + port);
					//sc.configureBlocking(false);
					sc.connect(addr);
				
					/*while(!sc.finishConnect()){
						System.out.println("Connecting to Storage server..");
					}*/
				
					ByteBuffer buf = ByteBuffer.allocate(50);
					buf.put(blkbyte);
					buf.flip(); //set position and limit
									
					//String blk = decoder.decode(buf).toString(); 
					System.out.println("Requesting for block:::" + blk);
					sc.write(buf);
					//sc.close();
					
					
					
					InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", port);
					SocketChannel sc1 = SocketChannel.open();
					//System.out.println("\nEstablishing connection with Storage server on port: " + port);
					//sc1.configureBlocking(false);
					sc1.connect(addr1);
					
					/*ByteBuffer buf11 = ByteBuffer.allocate(50);
					sc1.read(buf11);
					buf11.flip();
					int blkk = buf11.getInt();
					System.out.println("Client reading from server::" + blkk);*/
					
					FileOutputStream fout = new FileOutputStream("/DHPFS/Client/"+blk);
					FileChannel fcc = fout.getChannel();
					
					System.out.println("Reading Block from storage server..");
					ByteBuffer sbuf = ByteBuffer.allocate(1024 * 10000); //4.8MB
					
					sc1.read(sbuf);
					sbuf.flip();
					System.out.println("Block read from storage server!! ");
					while(sbuf.hasRemaining()){
						fcc.write(sbuf);
					}
									
					
					/*MappedByteBuffer sbuf = fcc.map(MapMode.READ_WRITE, 0, fcc.size());
					sc1.read(sbuf);
					sbuf.flip();
					System.out.println("Block read from storage server!! ");
					while(sbuf.hasRemaining()){
						fcc.write(sbuf);
					}*/
					
					//sbuf.clear();//m
					fcc.close();
					fout.close();
					sc.close();
					sc1.close();
					
			}catch(Exception e){ System.out.println("new client Exception:" +e.getMessage());}
			
		}
		
	

		public static void main(String argv[]) {
			
			if(argv.length != 2){
			 System.err.println("Required Filename and SIO/NIO to read!!");
			}
			// get Meta-data
	       	Client obj = new Client();
	        String filename = argv[0];
	        obj.ConnectToMetaserver(filename);
	        
	             
	        String command1 = argv[1];
	        if( command1.equals("SIO") ) {
	        	
	            // Read blocks from Storage server
	        	Client obj1 = new Client();
	        	long start = System.currentTimeMillis();
	        	for (Map.Entry<String, ArrayList<Object>> e : bport1.entrySet()){
				
	        	
	        		String sblkname = e.getKey();
	        		String shost = e.getValue().get(0).toString();
	        		int sport = Integer.parseInt(e.getValue().get(1).toString());
				
	        		//System.out.println("gettt-->"+ e.getKey() + ": " + e.getValue().get(0).toString()+ "::" + e.getValue().get(1));
	        		//Connects to storage server to read list of blocks
	        		obj1.ConnectToStorageserverSIO(sblkname,shost,sport);
	        	}
	        	long end = System.currentTimeMillis();
	        	System.out.println("\n*******Read response time for file(Client1 -> Storage server)::::" + (end-start));
	        }
	        
        if( command1.equals("NIO") ) {
        	
        	// Read blocks from Storage server
        	Client obj2 = new Client();
        	long start1 = System.currentTimeMillis();
        	for (Map.Entry<String, ArrayList<Object>> e : bport1.entrySet()){
			
        	
        		String sblkname = e.getKey();
        		String shost = e.getValue().get(0).toString();
        		int sport = Integer.parseInt(e.getValue().get(1).toString());
			
        		//System.out.println("gettt-->"+ e.getKey() + ": " + e.getValue().get(0).toString()+ "::" + e.getValue().get(1));
        		//Create new thread for each request
        		obj2.ConnectToStorageserverNIO(sblkname,shost,sport);
        	}
        	long end1 = System.currentTimeMillis();
        	System.out.println("\n*******Read response time for file(Client -> Storage server)::::" + (end1 - start1));
        }
	}
	
}
