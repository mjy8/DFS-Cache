import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
/*
 * Meta_Service will create list of storage servers(host/port),
 * Block table which maps block-id to host/port,
 * listen on 2 ports, 7777 to serve  meta-data request for clients from C/B table, 
 * 5555 to update meta-data on C-table
 */

public class Meta_Service extends Meta_Server{

	
	//public static List<String> S_host; 
	//public static List<Integer> S_port;
	//need not intialize var again
	
	static String Host_Meta = "localhost";
	protected static final int Port_Meta = 7777;
	protected static final int Port_MetaUpd = 5555;
    static int Storage_Servers_No;
	static int Meta_Server_No = 1;
	public static final int buff_size = 1024 * 100; //1 MB
	byte[] arraybyt;
	
	public static ArrayList<Object> hport;
	public static HashMap<String,ArrayList<Object>> bport = new HashMap<String,ArrayList<Object>>(); //(Disk block -> host/port)
	FileOutputStream fos = null;
	static ObjectOutputStream oos= null;
	static ObjectOutputStream ooc = null;
	static int blkupdobj = 0;
	
	ServerSocket clisten;
	ServerSocket clisUpd;
	
	public static LinkedList<String> Mrublk1 = new LinkedList<String>(); //List of blks in cache
	public static HashMap<String, ArrayList<Object>> mblk1 = new HashMap<String, ArrayList<Object>>(); // (blkname -> blkid,host,port)
	//HashMap or object should be static, to get values
	public Meta_Service(){
		
		 try { 
	            clisten = new ServerSocket(Port_Meta);
	            System.out.println ("Listening on port: " + clisten.getLocalPort());
	            clisUpd = new ServerSocket(Port_MetaUpd);
	            System.out.println ("Listening on port: " + clisUpd.getLocalPort());
	        } catch(Exception e) {System.out.println("clisten::"+ e.getMessage());}
	        
	}
	
	//get Metadata() for Client	
	public void Client_serviceRequest() {
		//XX create a thread to run this connection
		try {
				
			for(;;){
				//Accept client connection
				Socket csock = clisten.accept();
				 
				BufferedReader iread = new BufferedReader(new InputStreamReader(csock.getInputStream()));
				//BufferedWriter iout = new BufferedWriter(new OutputStreamWriter(csock.getOutputStream()));

				String blk = iread.readLine();
				System.out.println("\nClient requesting metadata for block::" + blk);
				
				iread.close();
				csock.close();
				
				//Get Meta data for req file
				GetMetaBlk(blk); 
				//call bport.get(blk), write server loc to sock
			}
			
		}catch(Exception e) {System.out.println("Meta Exception:" +e.getMessage());}
		
	}
	public void GetMetaBlk(String bnam){
		
		try {	
				//Check block exists in Cache-table (Linked list or Map) 
				//Mrublk1- will have blkid list and mblk1 contains blkname as key ->(blkid,host,port)
				if(mblk1.containsKey(bnam)) {
					String blkid = mblk1.get(bnam).get(0).toString();
					if(Mrublk1.contains(blkid)) {
						System.out.println("\n Cache blks in list and map:" + Mrublk1 + ":::" + mblk1);
					
						System.out.println("\n\n*** Requested block exists in Coopertive cache client !!!:" + mblk1.get(bnam));
						Socket csock6 = clisten.accept();
						ObjectOutputStream oblk = new ObjectOutputStream(csock6.getOutputStream());
						oblk.writeObject(mblk1);
					
						csock6.close();
						oblk.close();
					
					//clisten.close();//Close server socket for 7777
					}
					//Check if block exists in B-table
					//bport maps 
					else if(bport.containsKey(bnam)){
						System.out.println("\n\n *** Requested block exists in Storage server ! :" + bport.get(bnam));
						Socket csock6 = clisten.accept();
						ObjectOutputStream oblk = new ObjectOutputStream(csock6.getOutputStream());
						oblk.writeObject(bport);
						
						csock6.close();
						oblk.close();
						
						clisten.close();//Close server socket for 7777
					}
				}
				//Check block exists in B-table (Storage server)
				else if(bport.containsKey(bnam)){
					
					System.out.println("\n\n *** Requested block exists in Storage server ! :" + bport.get(bnam));
					Socket csock6 = clisten.accept();
					ObjectOutputStream oblk = new ObjectOutputStream(csock6.getOutputStream());
					oblk.writeObject(bport);
					
					csock6.close();
					oblk.close();
					
					clisten.close();//Close server socket for 7777
				}
				else {
					System.out.println("\n\n XX Requested block/meta-data does not exist in Meta server XX");
				}
				
				//Meta_Service req4 = new Meta_Service();
				//req4.start();	
				
			}catch(Exception e) {System.out.println("Exception(Meta service cache..):" +e.getMessage());}
			
			
		}
	
	public void Client_Update() {
		
		try {
			//synchronized(this){		
			for(;;){
				
				//Accept client connection
				Socket csocU = clisUpd.accept();
				System.out.println("Updating Meta:: C-Table.......");
				ObjectInputStream ctab = new ObjectInputStream(csocU.getInputStream());
				
				//updating C-table list
				LinkedList<String> Mrublk2 = (LinkedList) ctab.readObject();
				//Mrublk1 = new LinkedList<String>(Mrublk2);
				if(Mrublk2.size() != 0) {
					
					Mrublk1.add(Mrublk2.get(0));
				}else {
					Mrublk1 = new LinkedList<String>(Mrublk2);
				}
				// ????? need to read arraylist<object> which maps blkname -> <blkID, host,port>
				
				//Updating C-table map (blk->copclient)
				HashMap<String, ArrayList<Object>> mblk2 = (HashMap) ctab.readObject();
				//mblk1 = new HashMap<String, Object> (mblk2);
				if(mblk2.size() !=  0) {
					for (Map.Entry<String, ArrayList<Object>> e : mblk2.entrySet()){
					
						mblk1.put(e.getKey(), e.getValue());
					}
				}else {
					
					mblk1 = new HashMap<String, ArrayList<Object>>(mblk2);
				}
				System.out.println("\n\n Update C-table in Meta server::: " + Mrublk1 +"--" + mblk1 );
				
		
				csocU.close();
				ctab.close();
				//commented to test -while updating after expire blks
				//clisUpd.close();  
				
			    // Here it loops around to find next update request,
				//Since multiple threads shouldn't update simultaneously(Data inconsistency)  
				//Again listen on port 7777 and 5555 to handle req from clients
				Meta_Service req1 = new Meta_Service();
				req1.start();
		
			}
			//}
		}catch(Exception e) {System.out.println("Meta Exception Update table:" +e.getMessage());}
			
	}
	
	public void run() {
		
		Client_serviceRequest();
		Client_Update();
	}
	
	
	public static void main(String args[]){
		
		Storage_Servers_No = Integer.parseInt(args[0]);
		//add storage servers
		Meta_Server add = new Meta_Server();
		add.Add_StorageServer(Storage_Servers_No);
		try {	
			//creating list of host/port
			for(int i=0; i < Storage_Servers_No; i++) {
				hport = new ArrayList<Object>(); //each time creating new instance or obj
				hport.add(S_host.get(i));
				hport.add(S_port.get(i));
				//System.out.println("hportt:" + hport);
				//Map blocks into storage servers(host/port)
				bport.put("Blk"+i+".sim", hport);
			}
		}catch(Exception e) {System.out.println("Exception(creating Meta file..):" +e.getMessage());}
		System.out.println("Btable updated(block name->host/port)::" + bport);
		
		//listening on port
		Meta_Service req = new Meta_Service();
		req.start();
	}
}
