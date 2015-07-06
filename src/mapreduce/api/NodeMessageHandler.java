package mapreduce.api;

public interface NodeMessageHandler {

	public void handle(NodePacketBase nodePacket);
}
