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


public class Client_Service extends Client{

	int mport = 7777; // Meta server- getting block
	int mportu = 5555; //Meta server-  updating C-table
	static String Host_Client = "localhost";
	protected static int Port_Client;
	
	static ServerSocket cachelisten;
	public static HashMap<String, ArrayList<Object>> mapblk = new HashMap<String, ArrayList<Object>>(); // blk->Copclient
	public ArrayList<Object> hport11;
	
	
	public Client_Service() {
		//boolean cdir = (new File("/DHPFS/Client")).mkdir();
		arraybyt1 = new byte[buff_size];
		try { 
            cachelisten = new ServerSocket(Port_Client);
            System.out.println ("Listening on port for cache req: " + cachelisten.getLocalPort());
            
        } catch(Exception e) {System.out.println("Client cache blk listen 222::"+ e.getMessage());}
		
	}
	
	
	public void GetBlock(String blkname){
		
		//connect to meta server write blkname
		//get storage server locn
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
			clnt.close();
			
			//Get block location from meta server
			Socket clnt5 = new Socket(hostname, mport);
			ObjectInputStream ooi = new ObjectInputStream(clnt5.getInputStream());
			HashMap<String,ArrayList<Object>> bport6 = (HashMap) ooi.readObject();
			System.out.println("\n\nReceived block location from Meta_service:" + bport6.get(blkname) );
			//System.out.println("host/port value:" + bport6.get(blkname).get(0)+ ":::" + bport6.get(blkname).get(1));
			
			
			ooi.close();
			clnt5.close();
			
			//check if array list as 3 (blkid, host, port) - read unique blkid else 2 - (host,port) 
			// if 3 - write blkid instead of blkname and call cacheblk(blkid)
			//if 2 - create blkid (blkname+port) and call cacheblk(blkid)
			//Based on meta-data, client try to read from storage/client cooperative cache
			
			//Connect to storage server to read block 
			if(bport6.get(blkname).size() == 2) {
				String hosts = bport6.get(blkname).get(0).toString();
				int ports = Integer.parseInt(bport6.get(blkname).get(1).toString());
				Client obj6 = new Client();
				System.out.println("\n\n Client connecting to storage server to read blocks...");
				
				//blocks are read from storage and kept(/DHPFS/Client)
				long start = System.currentTimeMillis();
				obj6.ConnectToStorageserverSIO(blkname, hosts, ports);
				long end = System.currentTimeMillis();
	        	System.out.println("\n*******Block access time (Client -> Storage server)::::" + (end-start));
	        	
			
	        	// If blk is read then only cache else exit
				//clnt5.close();
				//ooi.close();
				//Create a unique blkid and then Cache Blocks
				String blkid = blkname+ports;
				Cache_Block(blkname, blkid);
			}
			//Connect to Client cooperative cache 
			else if(bport6.get(blkname).size() == 3) {
				
				//Reading (blkid,host,port) from Meta server
				String blkid = bport6.get(0).toString();
				String hosts = bport6.get(blkname).get(1).toString();
				int ports = Integer.parseInt(bport6.get(blkname).get(2).toString());
				Client obj6 = new Client();
				System.out.println("\n\n Client connecting to Client cooperative cache to read blocks...");
				
				//Connecting to client to read cache blocks
				long start1 = System.currentTimeMillis();
				obj6.ConnectToStorageserverSIO(blkid, hosts, ports);
				long end1 = System.currentTimeMillis();
				System.out.println("\n*******Block access time (Client -> Cooperative cache)::::" + (end1-start1));			
				// If blk is read then only cache else exit
				clnt5.close();
				ooi.close();
				//Create a unique blkid and then Cache Blocks
				//String blkid = blkname+ports;
				Cache_Block(blkname,blkid);
							
			}
			
		}catch(Exception e){ System.out.println("Exception:" +e.getMessage());}
	   
	}
	
	// Put blocks in cache and update Meta server
	public void Cache_Block(String blkname, String blkid) {
		
	try {
			
			//Create tuple and Put Blocks in memory (Call Blkcache)
			Blkcache cblk = new Blkcache();
			System.out.println("\nPutting blocks in memory::::" + blkid);
			cblk.PutBlk(blkid);
			//Thread.sleep(3000);
			// Map cache blk
			hport11 = new ArrayList<Object>();
			
			hport11.add(blkid);
			hport11.add(Host_Client);
			hport11.add(Port_Client);
			System.out.println("\nClient host/port::" + hport11);
			
			//  It should mapblk to put <blkname -> <blkID, host,port>>
			// Also Mrublk should add blkname instead of ID, since its written to Meta
			mapblk.put(blkname, hport11);
			//Also update linked list - which holds cached block list
			LinkedList<String> Mrublk2 = new LinkedList<String>(cblk.Mrublk);
		
					
			//Update Meta server or C-table
			Socket clntU = new Socket(hostname, mportu);
			//PrintWriter mout22 = new PrintWriter(clntU.getOutputStream (),true);
			System.out.println("\nEstablishing connection with Meta server to update C-table on port: " + mportu);
			
			ObjectOutputStream objw = new ObjectOutputStream(clntU.getOutputStream());
			System.out.println("\n\n Mapblks and Mrublk list:::" + Mrublk2 + mapblk );
			//Writing blkname linked list and mapped blkname->blkid,host/port
			objw.writeObject(Mrublk2);
			objw.writeObject(mapblk);
			
			clntU.close();
			objw.close();
			
			//Listen on port to get cache client req
			cachelisten.close();
			//Client_Service objcc = new Client_Service();
			//objcc.start();
			
		
		}catch(Exception e){ System.out.println("Client_Service Exception:" +e.getMessage());}
		
		
	}
	
	//To server other client requesting for cached blocks 
	public void  ClientReq_Cache(){
		
	try {	
		 
		for(;;){
			
				//cachelisten = new ServerSocket(Port_Client);
				//System.out.println ("Listening on port for cache req: " + cachelisten.getLocalPort());
	         
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
					ObjectOutputStream cacos = new ObjectOutputStream(cacs2.getOutputStream());
					System.out.println("Coopertive writing block from cache........");
					cacos.write(cblk.GetBlk(blk));
					//cacos.writeObject(cblk.mblk.get(blk));
					
					cacs2.close();
					cacos.close();
					//cachelisten.close();
					//Client_Service objcc = new Client_Service();
			        //objcc.start();
				}
				else {
					   System.out.println("\n Cache blk expired / does not exist XX");
					   System.out.println("\n Try to access block from Storage server...");
					   //XXDo 1: Try to read block from storage server if blocks are evicted
					   //Connects to Meta_server, then storage server to read block, cache block
					   // Client_Service1 should now retry after few seconds, where block exists in client cache
					   
					   //XXDO 2: Meta_service should return block handle that contains block locations
					   // from both client_service cache and storage server; So initially it contacts 
					   // client_service cache else if block not present contact storage server
					   
					   Client_Service reqobj = new Client_Service();
					   reqobj.GetBlock(blk);
					   
				}
				
				//cachelisten.close();
				//Client_Service objcc1 = new Client_Service();
		        //objcc1.start();
				
		}
		
        } catch(Exception e) {System.out.println("Client cache blk listen...XXXX::"+ e.getMessage());}
        
        //Only for Experiment 2 
        //Client_Service objcc = new Client_Service();
        //objcc.start();
	}
	
	public void run() {
		
		ClientReq_Cache();
	}
	
	
	public static void main(String args[]) {

		//Coop client port that listen
		Port_Client = Integer.parseInt(args[0]);
		
		Client_Service obj = new Client_Service();
		String blkname = args[1];
    	obj.GetBlock(blkname);
		
		//For Experiment2 - To cache multiple blocks
    /* try {  	
    	 for(int i=0; i < 2; i++){
        	Client_Service obj11 = new Client_Service();
        	//String blkname = args[1];
        	obj11.GetBlock("Blk"+i+".sim");
        	cachelisten.close();
        }
     }
     catch(Exception e ) { System.out.println("\n cannot close cache listen socket..." + e);}*/
     
     // get Meta-data
       /*	Client_Service obj1 = new Client_Service();
        String blkname1 = args[2];
        obj1.GetBlock(blkname1);*/
        
        Blkcache cob  = new  Blkcache();
        System.out.println("\n\n Blocks that are cached at Cooperative client:::" + cob.Mrublk);
       // System.out.println("\n\n Blocks that are mapped at Coopert client:::" + cob.mblk);
        
        /*
        While performing experiments, to test client cooperative cache: 
		after client1 read blk from storage server, put them in client cooperative cache.
       	*wait for few seconds, before running client2 which try to access blk from cache
         */
     
        //For experiment 2
        //Listen on port to get cache client req
		Client_Service objcc = new Client_Service();
		objcc.start();
		
	}
	
	
	
}
