import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KPLProducer {

    private static int counter = 0;

    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException, ExecutionException {

        if (args.length == 1) {
            counter = Integer.parseInt(args[0]);
        } else {
            counter = 100;
        }

        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRecordMaxBufferedTime(3000)
                .setMaxConnections(1)
                .setRequestTimeout(60000)
                .setRegion("us-west-2");

        final com.amazonaws.services.kinesis.producer.KinesisProducer kinesisProducer = new com.amazonaws.services.kinesis.producer.KinesisProducer(config);

//        pushRecordSynch(kinesisProducer);
        putRecordAsnch(kinesisProducer);


    }

    private static void putRecordAsnch(com.amazonaws.services.kinesis.producer.KinesisProducer kinesisProducer) throws UnsupportedEncodingException, InterruptedException {

        FutureCallback<UserRecordResult> myCallback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                /* Analyze and respond to the failure  */
                System.out.println("Can not Put record into shard got some errors");
            }

            ;

            @Override
            public void onSuccess(UserRecordResult result) {
                /* Respond to the success */
                System.out.println("Put record into shard " +
                        result.getShardId() + " " + result.getSequenceNumber());
            }

            ;
        };
        for (int i = 0; i < counter; ++i) {
            long createTime = System.currentTimeMillis();
            Date currentDate = new Date(createTime);
            DateFormat df = new SimpleDateFormat("MM:dd:yy:HH:mm:ss");
            ByteBuffer data = ByteBuffer.wrap(("myData" + String.valueOf(i)).getBytes("UTF-8"));
            String partitionkey = String.format("partitionKey-%s", df.format(currentDate).toString());
            ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord("ExampleInputStream", partitionkey, data);
            // If the Future is complete by the time we call addCallback, the callback will be invoked immediately.
            Futures.addCallback(f, myCallback);
        }

        Thread.sleep(5000);
    }

    private static void pushRecordSynch(com.amazonaws.services.kinesis.producer.KinesisProducer kinesisProducer) throws UnsupportedEncodingException, InterruptedException, ExecutionException {
        List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();
        for (int i = 0; i < counter; i++) {
            ByteBuffer data = ByteBuffer.wrap("myData".getBytes("UTF-8"));
            // doesn't block
            putFutures.add(
                    kinesisProducer.addUserRecord("ExampleInputStream", "myPartitionKey", data));
        }

        for (Future<UserRecordResult> f : putFutures) {
            UserRecordResult result = f.get(); // this does block
            if (result.isSuccessful()) {
                System.out.println("Put record into shard " +
                        result.getShardId() + result.getSequenceNumber());
            } else {
                for (Attempt attempt : result.getAttempts()) {
                    System.out.println("failed to write");
                }
            }
        }
    }

}
