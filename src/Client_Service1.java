import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class Client_Service1 extends Client{

	int mport = 7777; // Meta server- getting block
	int mportu = 5555; //Meta server-  updating C-table
	static String Host_Client = "localhost";
	protected static int Port_Client;
	
	ServerSocket cachelisten;
	public static Map<String, Object> mapblk = new HashMap<String, Object>(); // blk->Copclient
	public ArrayList<Object> hport11;
	
	
	public Client_Service1() {
		//boolean cdir = (new File("/DHPFS/Client")).mkdir();
		arraybyt1 = new byte[buff_size];
		/*try { 
            cachelisten = new ServerSocket(Port_Client);
            System.out.println ("Listening on port for cache req: " + cachelisten.getLocalPort());
            
        } catch(Exception e) {System.out.println("Client cache blk listen::"+ e.getMessage());}*/
		
	}
	
	
	public void GetBlock(String blkname){
		
		//connect to meta server write blkname
		//get storage serv locn
		//connect to storage server, read blk 
		//Read blk from storageB and map blk to coop client
		//update meta_service , update c-table 
		//listen on port to get req from other clients
		
		try {
			//Connect to Meta-server
			Socket clnt = new Socket(hostname, mport);
			PrintWriter mout = new PrintWriter(clnt.getOutputStream (),true);
			//Cannot perform read and write both on socket simulty
			
			System.out.println("\nEstablishing connection with Meta server on port: " + mport);
			System.out.println("Client:" + clnt);
			//Write to socket
			System.out.println("\n------------------Getting Block location from Meta Server-------------------->");
			System.out.println("\nClient getMetadata() from Meta-Server :-->" + blkname);
			mout.write(blkname);
					
			mout.close();
			
			//Get block location from meta server
			Socket clnt5 = new Socket(hostname, mport);
			ObjectInputStream ooi = new ObjectInputStream(clnt5.getInputStream());
			HashMap<String,ArrayList<Object>> bport7 = (HashMap) ooi.readObject();
			System.out.println("\n\nReceived block location from Meta_service:" + bport7.get(blkname) );
			//System.out.println("host/port value:" + bport7.get(blkname).get(0)+ ":::" + bport7.get(blkname).get(1));
			//System.out.println("\n Array list::" + bport7.get(blkname).size());
			
			//??check if array list as 3 - read unique blkid else 2 - host/port 
			// if 3 - write blkid instead of blkname and call cacheblk(blkid)
			//if 2 - create blkid (blkname+port) and call cacheblk(blkid)
			
			//Connect to storage server to read block
			if(bport7.get(blkname).size() == 2) {
			
				String hosts = bport7.get(blkname).get(0).toString();
				int ports = Integer.parseInt(bport7.get(blkname).get(1).toString());
				Client obj7 = new Client();
				System.out.println("\n\n Client connecting to storage server to read blocks...");
				obj7.ConnectToStorageserverSIO(blkname, hosts, ports);
			
				// If blk is read then only cache else exit
				clnt5.close();
				ooi.close();
				//Create a unique blkid and then Cache Blocks
				String blkid = blkname+ports;
				Cache_Block(blkname, blkid);
			}
			//Connect to Client cooperative cache 
			else if(bport7.get(blkname).size() == 3) {
				
				String blkid = bport7.get(blkname).get(0).toString();
				String hosts = bport7.get(blkname).get(1).toString();
				int ports = Integer.parseInt(bport7.get(blkname).get(2).toString());
				Client obj7 = new Client();
				System.out.println("\n\n Client connecting to Client cooperative cache to read blocks...");
				
				long start1 = System.currentTimeMillis();
				obj7.ConnectToStorageserverSIO(blkid, hosts, ports);
				long end1 = System.currentTimeMillis();
	        	System.out.println("\n*******Block access time (Client -> Cooperative cache)::::" + (end1-start1));
	        	
				// If blk is read then only cache else exit
				clnt5.close();
				ooi.close();
				//Create a unique blkid and then Cache Blocks
				//String blkid = blkname+ports;
				
				//** not needed when performing experiments to read blocks
				//String blkid1 = blkid+ports;
				//Cache_Block(blkname,blkid1);
				
				
			}
		}catch(Exception e){ System.out.println("client service Exception:" +e.getMessage());}
	   
	}
	
	
	public void Cache_Block(String blkname,String blkid) {
		
	try {
			//Create tuple and Put Blocks in memory (Call Blkcache)
			Blkcache cblk = new Blkcache();
			System.out.println("Putting blocks in memory::::" + blkid);
			cblk.PutBlk(blkid);
	
			// Map cache blk
			hport11 = new ArrayList<Object>();
			//?? add blkid
			hport11.add(blkid);
			hport11.add(Host_Client);
			hport11.add(Port_Client);
			System.out.println("Client host/port::" + hport11);
			
			// ??Mapping blk to cop-client //** It should be unique blkid 
			//  It should mapblk to put blkname -> <blkID, host,port>
			// Also Mrublk should add blkname instead of ID, since its written to Meta
			mapblk.put(blkname, hport11);
			LinkedList<String> Mrublk2 = new LinkedList<String>(cblk.Mrublk);
		
					
			//Update Meta server or C-table
			Socket clntU = new Socket(hostname, mportu);
			//PrintWriter mout22 = new PrintWriter(clntU.getOutputStream (),true);
			System.out.println("\nEstablishing connection with Meta server to update C-table on port: " + mportu);
			
			ObjectOutputStream objw = new ObjectOutputStream(clntU.getOutputStream());
			System.out.println("\n\n Mapblks and Mrublk list:::" + Mrublk2 + mapblk );
			
			objw.writeObject(Mrublk2);
			objw.writeObject(mapblk);
			
			clntU.close();
			objw.close();
			
			//Listen on port to get cache client req
			Client_Service1 objcc = new Client_Service1();
			//objcc.run();
			
		
		}catch(Exception e){ System.out.println("Exception:" +e.getMessage());}
		
		
	}
	
	
	/*public void  ClientReq_Cache(){
		
	try {	
		 cachelisten = new ServerSocket(Port_Client);
         System.out.println ("Listening on port for cache req: " + cachelisten.getLocalPort());
		for(;;){
				//Accept client connection for cache blk req
				Socket cacsock = cachelisten.accept();
				BufferedReader iread = new BufferedReader(new InputStreamReader(cacsock.getInputStream()));
				String blk = iread.readLine();
				System.out.println("\nClient requesting for cache block::" + blk);
				
				iread.close();
				cacsock.close();
				
				Blkcache cblk = new Blkcache();
				if(cblk.mblk.containsKey(blk)) {
					System.out.println("\n Cache blk exists in copclient::" + blk);
					
					Socket cacs2 = cachelisten.accept();
					OutputStream cacos = cacs2.getOutputStream();
					
					cacos.write(cblk.GetBlk(blk));
				}
				else {
					System.out.println("Cache blk expired / does not exist XX");
				}
				
		}
            
        } catch(Exception e) {System.out.println("Client cache blk listen::"+ e.getMessage());}
	}
	
	public void run() {
		
		ClientReq_Cache();
	}*/
	
	
	
	public static void main(String args[]) {

		//Coop client port that listen
		Port_Client = Integer.parseInt(args[0]);
		/*
        While performing experiments, to test client cooperative cache: 
		after client1 read blk from storage server, put them in client cooperative cache.
       	*wait for few seconds, before running client2 which try to access blk from cache
         */
		
		// get Meta-data
       	Client_Service1 obj = new Client_Service1();
        String blkname = args[1];
        obj.GetBlock(blkname);
	}
	
	
	
}
