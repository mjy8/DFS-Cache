
public interface ReadIO {

	public void ConnectToMetaserver(String filename);
	public void  ConnectToStorageserverSIO(String blkname, String host, int port);
	public void  ConnectToStorageserverNIO(String blkname, String host, int port);
	public void ConnectToCooperativecache(String blkname, String host, int port);
}
