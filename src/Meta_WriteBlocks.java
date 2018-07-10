import java.io.*;
import java.util.*;


public class Meta_WriteBlocks extends Meta_Server{

	static String linesep = System.getProperty("line.separator");
	public static ArrayList<String> blk = new ArrayList<String>();
	BufferedReader in;
    FileOutputStream fout;
    int length;
    int splitlength;
    String Fname;
	static int blkcnt =0;
	//static int Storage_Servers_No = 3;
	StringBuffer sbuf;
	
	public Meta_WriteBlocks() {
		
		boolean sdir = (new File("/DHPFS/storeblocks")).mkdir();
		boolean mdir = (new File("/DHPFS/Metadata")).mkdir();
	}
	
	
	// Split large files into blocks
	public void Splitfile(String fileName,int splitlen)   {
        try
        {
           // fin=new FileInputStream(fileName);
            in = new BufferedReader(new FileReader(fileName));
            Fname = fileName;
            length = 0;
            splitlength = splitlen;
            // Creates new block based on row split/length
            SplitBlk(Fname);

        }
        catch(FileNotFoundException e)
        {
            System.out.println("File not foundXX");
        }
        catch(IOException e)
        {
            System.out.println("IOException generated");
        }
    }
	
	// Creates new block based on row split/length
    public void SplitBlk(String Fname) {
    	int blkn = 1;
        try
        {
            int i=0;
            StringBuffer sbuf = new StringBuffer();
            BufferedReader in = new BufferedReader(new FileReader(Fname));
            String c=in.readLine();
            while(c != null)
            {
            	File file = new File("/DHPFS/storeblocks/"+ Fname+blkn);
            	//New block-id is added to Array_list 
            	blk.add(Fname+blkn);
        	    blkcnt++;
            	blkn++;
            	
                //FileOutputStream fw=new FileOutputStream(file);
            	BufferedWriter fy = new BufferedWriter(new FileWriter(file));
                while(c!= null && length < splitlength)
                {
                	sbuf.append(c);
    				sbuf.append(linesep);
                	fy.write(sbuf.toString());
                    
                    c = in.readLine();
                    sbuf = new StringBuffer();
                    length++;
                }
                length=0;
                fy.close();
                i++;
            }

        }
        catch(Exception e) { System.out.println("file split exception:" + e.getMessage());
            e.printStackTrace();
        }
    }
	
	
	public static void main(String args[]){
		
		Meta_WriteBlocks obj = new Meta_WriteBlocks();
		//Meta_Server obj1 = new Meta_Server();
		//obj1.Add_StorageServer();
		if(args.length != 3){
			System.err.println("--->Required filename, split(rows), and No_storage_servers");
			return;
		}
		
		String filename = args[0];
		int size = Integer.parseInt(args[1]);
		Storage_Servers_No = Integer.parseInt(args[2]);
		try {
			
			obj.Splitfile(filename, size);
			} catch (Exception e) { System.out.println("Split Exception:" +e.getMessage());}
		
		System.out.println("----------------File Split Done!!---------------------------------->");
		System.out.println("\n");
		
		//Creating Block Table <filename->block_id1,2..n>
		Meta_Server btab = new Meta_Server();	
		btab.Create_Blocktable(filename, blk);
		
		System.out.println("--------------Creating Block Table------------------------------------>");
		System.out.println("\n");
		
		try {
		
			//Add Storage Servers
			Meta_Server add = new Meta_Server();
			add.Add_StorageServer(Storage_Servers_No);
			System.out.println("------------------Adding Storage Servers-------------------------------->");
			System.out.println("\n");
		
			//Distribute Blocks to Storage servers(Round Robin)
			Meta_Server sto = new Meta_Server();
			for(int i=0,blkn =0,k =0; i < Storage_Servers_No || blkn <= blk.size();) {
			
				sto.ConnectToStorageserver(i,blk.get(blkn), blkcnt);
				blkn++;
				i++;
				if(i == (Storage_Servers_No))
					i = (i % (Storage_Servers_No ));
					
				System.out.println("\n");
			/*
			//Update Btable
			Meta_Server utab = new Meta_Server();
			hport.add(S_host.get(blkn));
			hport.add(S_port.get(blkn));
			utab.Update_Btable(blk.get(blkn), hport);
			blkn++;*/
		}
		
		}catch (IndexOutOfBoundsException e) {	System.out.println("Emessg:: Block Distribution done!!");}
	
		/*Meta_Server lup = new Meta_Server();
		lup.LookupBlock(filename);
		
		try {
			lup.DisplayMetadata();
		} catch(Exception e ) { System.out.println("Display meta: " + e.getMessage() ); }*/
		
		//Meta server listen on port to serve client meta-data request
		Meta_Server clientreq = new Meta_Server(Port_Meta);
		
		clientreq.start();
		
	}
	
}
