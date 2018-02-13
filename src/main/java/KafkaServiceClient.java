import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import prices.KakfaStateService;

import java.util.Map;

public class KafkaServiceClient {

    public static void main(String [] args) {
        try {
            TTransport transport;

            transport = new TSocket("54.252.228.165", 9090);
            transport.open();

            TProtocol protocol = new TJSONProtocol(transport);
            KakfaStateService.Client client = new KakfaStateService.Client(protocol);

            perform(client);

            transport.close();
        } catch (TException | InterruptedException x) {
            x.printStackTrace();
        }
    }

    private static void perform(KakfaStateService.Client client) throws TException, InterruptedException {
        Map<String, Long> kv = null;

        while (true) {
            kv = client.getAll("basket-store");
            kv.forEach((k, v) -> System.out.println("Key is " + k + " and value is " + v));
            Thread.sleep(10000);
        }
    }
}