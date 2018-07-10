import java.net.*;
import java.io.*;
import java.util.*;



public class Meta_Server extends Thread implements Serializable{
	
	
	public static List<String> S_host; 
	public static List<Integer> S_port;
	
	static String Host_Meta = "localhost";
	protected static final int Port_Meta = 8888;
    
    static int Storage_Servers_No; //y
	static int Meta_Server_No = 1;
	public static final int buff_size = 1024 * 100;
	byte[] arraybyt;
	
	public ArrayList<Object> hport; //<host,port>
	public static Map<String, ArrayList<String>> btable = new HashMap<String,ArrayList<String>>(); //(filename, block list)
	public static HashMap<String,ArrayList<Object>> bport = new HashMap<String,ArrayList<Object>>(); //(block, host/port)
	FileOutputStream fos = null;
	static ObjectOutputStream oos= null;
	static ObjectOutputStream ooc = null;
	static int blkupdobj = 0;
	
	ServerSocket clisten;
	
	public Meta_Server() {
		
		arraybyt = new byte[buff_size];
	}
	
	public Meta_Server(int Port_Meta){
				
		 try { 
			    // Meta_server listen on port-8888 to server client request
	            clisten = new ServerSocket(Port_Meta);
	            System.out.println ("Listening on port: " + clisten.getLocalPort());
	        
	         } catch(Exception e) {System.out.println("clisten::"+ e.getMessage());}
    }
	
	// Add Storage servers <list of hosts/port> to distribute blocks
	public void Add_StorageServer(int Storage_Servers_No) {
		
		S_host = new ArrayList<String>();
		S_port = new ArrayList<Integer>();
		for(int i=0; i < Storage_Servers_No; i++)	
			S_host.add("localhost");
		
		for(int i=0; i < Storage_Servers_No; i++)
			S_port.add(4000+i);
		System.out.println("Storage Servers connected::" + S_host);
		System.out.println("Storage Server Ports::" + S_port);
	}
		
	//Connecting to Storage Servers (Distribute Blocks)
	public void ConnectToStorageserver(int slist, String blkname, int blkcnt )  {
		
		//Update Block Table
		hport = new ArrayList<Object>(); //each time creating new instance or obj
		hport.add(S_host.get(slist));
		hport.add(S_port.get(slist));
		Update_Btable(blkname, hport, slist,blkcnt);
		
		try {
			//Creating Storage server socket
			Socket sserver = new Socket(S_host.get(slist), S_port.get(slist));
			//cannot call func/do update here
			
			System.out.println("Establishing connection with server on port: " + S_port.get(slist) );
			System.out.println("Blkname::" + blkname);
			
			//Read or write blocks
			File fname = new File("/DHPFS/storeblocks/"+blkname);
			BufferedInputStream sin = new BufferedInputStream(new FileInputStream(fname));
			
			//BufferedOutputStream sout = new BufferedOutputStream(sserver.getOutputStream());
			OutputStreamWriter sout1 = new OutputStreamWriter(sserver.getOutputStream());
			sout1.write(blkname);
			//sout1.write("\n");
			sout1.close();
		
			/*sin.read(arrybyte, 0, arrybyte.length);
			//byte[] arrybyte = new byte[(int)fname.length()];
			//OutputStream sout = sserver.getOutputStream();
            //sout.write(arrybyte, 0, arrybyte.length);*/
            
			Socket sserver1 = new Socket(S_host.get(slist), S_port.get(slist));
			BufferedOutputStream sout = new BufferedOutputStream(sserver1.getOutputStream());
			int rlen = 0;
			while ((rlen = sin.read(arraybyt)) > 0) {
				sout.write(arraybyt, 0, rlen);
				//System.out.println("-------------Block Distributed--------------------->");
			}
			sin.close();
			sout.flush();
			sout.close();
			sserver.close();
			sserver1.close();
						
			//update b table - shld do
			//XXX
			//bport = new HashMap<Integer,HashMap<String,Integer>>();
			//bport.put(blkname, hostport.);
		}catch(Exception e){ System.out.println("Exception(Storage server):" +e.getMessage());}
	}
	
	// Updates B-Table <filename-->blkid,1,2,..n>
	public void Create_Blocktable(String filename, ArrayList<String> blklist) {
		
		try {
			    ///Creates Meta file
				btable.put(filename, blklist);
				System.out.println("Btable list:" + btable);
				fos = new FileOutputStream("/DHPFS/Metadata/"+filename);
				oos = new ObjectOutputStream(fos);
				oos.writeObject(btable);
				//metaout.close();
		
			}catch (IOException e) {System.out.println("Exception(creating Meta file):" +e.getMessage());}
	}
		
	//Update Block table after distributing blocks into storage servers
	public void Update_Btable(String bnam, ArrayList<Object> hp, int slist, int blkcnt ){
		
		try {
				bport.put(bnam, hp);
				blkupdobj++;
				System.out.println("Btable updated(block name->host/port)::" + bport);
				
				if( blkupdobj == blkcnt)
					oos.writeObject(bport);
				//oos.close();
			
			}catch(IOException e) {System.out.println("Exception(creating Meta file..):" +e.getMessage());}
		
		//hp.remove(slist);
		//slist++;
		//hp.remove(slist);
	}
	
	public void DisplayMetadata() throws Exception{
		
		try {
			FileInputStream fis = new FileInputStream("/DHPFS/Metadata/LFile.sim");
			ObjectInputStream ois = new ObjectInputStream(fis);
			Map btable = (Map) ois.readObject();
			System.out.println("Metadata blk list::" + btable);
			HashMap bport = (HashMap) ois.readObject();
			System.out.println("Metadata host/port list::" + bport);
			
			}catch(IOException e) {System.out.println("Exception(Display Meta file..):" +e.getMessage());}
	}
	
	//get Metadata() for Client	
	public void ClientRequest() {
		//XX create a thread to run this connection
		try {
				
			for(;;){
				//Accept client connection
				Socket csock = clisten.accept();
				//Perform read on Socket (filename) 
				BufferedReader iread = new BufferedReader(new InputStreamReader(csock.getInputStream()));
				BufferedWriter iout = new BufferedWriter(new OutputStreamWriter(csock.getOutputStream()));

				String fline = iread.readLine();
				System.out.println("Client requesting metadata for file::" + fline);
				
				iread.close();
				csock.close();
				
				//Get Meta data for requested file
				getMetadata(fline);
				
				//LookupBlock(fline);
				/*Socket csock1 = clisten.accept();
				PrintWriter out = new PrintWriter(csock1.getOutputStream (),true);
                out.println("Written to client Socket:" + new Date());
                			
				out.close();
				csock1.close();*/
				//After accepting each client request, it loops around to server next request
							
			}
					
		}catch(Exception e) {System.out.println("Meta Exception:" +e.getMessage());}
		
	}
	
	//get metadata() to client
	public void getMetadata( String fname) throws Exception {
		
		File mfile = new File("/DHPFS/Metadata/"+fname);
		boolean Exist = mfile.exists();
		if(Exist) {
			System.out.println("Req Meta File exists in Meta server !!-->" + fname);
			try {
					FileInputStream fis = new FileInputStream("/DHPFS/Metadata/" +fname);
					ObjectInputStream ois = new ObjectInputStream(fis);
					Map btable = (Map) ois.readObject();
					System.out.println("Metadata blk list::" + btable);
					HashMap bport = (HashMap) ois.readObject();
					System.out.println("Metadata host/port list::" + bport);
					
					//Writes Meta-data on to client output_stream
					Socket csock1 = clisten.accept();
					ObjectOutputStream ooc = new ObjectOutputStream(csock1.getOutputStream());
					ooc.writeObject(btable);
					ooc.writeObject(bport);
					
					csock1.close();
					ooc.close();
					ois.close();
					fis.close();
				}catch(IOException e) {System.out.println("Exception(Meta file..):" +e.getMessage());}
		}
		else {
				System.out.println("XX Req Meta File does not exist in Meta server XX-->" + fname);
		}
	}
	
	//Lookup block list for clients
	public void LookupBlock(String filename) {
		
		
		if (btable.containsKey(filename)) {
			
			//System.out.println("!! Requested File Exists in Block table::" +filename);
			//System.out.println("File --> Block list::" + btable.values());
			System.out.println("----------------------Block Table---------------------------->");
			//It should return block -> host/port mapping to client
			for (Map.Entry<String, ArrayList<String>> e : btable.entrySet()){
				
			    System.out.println(e.getKey() + ": " + e.getValue());
			    for(String blki : e.getValue()){
			    	//	System.out.println("Blkname:" + blki );
			    	System.out.println("Block list map host/port::"+blki + "-->"+bport.get(blki));
			    	//return bport.get(blki).toString();
			    }
			}
		}
		else {
			System.out.println("XX File not found in Block table XX::" +filename);
			//return null;
			}
	}
	
	public void run() {
		//Accepts client connection,for meta-data request
		ClientRequest();
	}
	
	public static void main(String args[]) {
		
		/*Meta_Server add = new Meta_Server();
		Meta_Server store = new Meta_Server();
		add.Add_StorageServer();
		int blkname =1;
		for(int i=0; i <= Storage_Servers_No; i++) {
			
			store.ConnectToStorageserver(i,blkname);
			blkname++;
			//Round robin for distributing blocks
			/*	if(i == Storage_Servers_No) {
				i = (i % Storage_Servers_No);
			}
				
		}*/
		Storage_Servers_No = Integer.parseInt(args[0]);
		// Meta_server listen on port-8888 to server client request
		Meta_Server clientreq = new Meta_Server(Port_Meta);
		
		clientreq.start();
		//cfile.ClientRequest();
	
	}	
	
}
