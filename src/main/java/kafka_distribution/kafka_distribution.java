package kafka_distribution;

import java.io.UnsupportedEncodingException;  
import java.util.Properties;  
import java.util.Random;  
import java.util.concurrent.Future;  
  
import org.apache.kafka.clients.producer.*;  
import org.opencv.core.Core;  
import org.opencv.core.Size;  
import org.opencv.core.Mat;  
import org.opencv.videoio.VideoCapture;  
  
public class kafka_distribution implements Runnable {  
  
    private boolean tracking = true;  
    private String brokerUrl = "10.75.161.54:9092";  
    private String topic = "ddp_video_source";  
    private String cameraUrl;
  
    public void Init(String cameraUrlIn, String brokerUrlIn, String topicIn) {  
        cameraUrl = cameraUrlIn;  
        brokerUrl = brokerUrlIn;  
        topic = topicIn;  
    }  
  
    public void run() {  
        boolean isOpen = false;  
        VideoCapture capture = null;  
  
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);  
  
//        try {  
//            capture = new VideoCapture();  
//            isOpen = capture  
//                    .open(cameraUrl);  
//        } catch (Exception e) {  
//            e.printStackTrace();  
//        }  
        int count = 0;  
        while (!isOpen) {  
            capture = new VideoCapture();  
            isOpen = capture  
                    .open(0);  
            System.out.println("Try to open times: " + ++count);  
        }  
  
        if (!isOpen) {  
            System.out.println("not open the stream!");  
            return;  
        }  
  
        Mat frame = new Mat(new Size(640, 480), 16);//new Size(640, 480), 16  
  
        Properties props = new Properties();  
        props.put("bootstrap.servers", brokerUrl);  
        props.put("metadata.broker.list", brokerUrl);  
        props.put("acks", "all");  
        props.put("client.id", "DemoProducer");  
        props.put("key.serializer",  
                "org.apache.kafka.common.serialization.StringSerializer");  
        props.put("value.serializer",  
                "org.apache.kafka.common.serialization.ByteArraySerializer");  
  
  
        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(  
                props);  
  
        Future<RecordMetadata> a = null;  
  
        long frameCount = 0;  
  
        while (tracking) {  
  
            capture.read(frame);  
//            org.opencv.imgcodecs.Imgcodecs.imwrite("/tmp/camera" + new Random().nextInt(100) + ".pgm", frame);  
            // frameCount;  
            byte[] frameArray = new byte[((int) frame.total() * frame  
                    .channels())];  
            frame.get(0, 0, frameArray);  
  
            System.out.println("FrameSize:" + frameArray.length);  
            System.out.println("Mat:height " + frame.height());  
            System.out.println("Mat: width " + frame.width());  
            System.out.println("channels: " + frame.channels());  
            System.out.println("type: " + frame.type());  
//          Mat formatFram = new Mat();  
//          frame.convertTo(formatFram, 0);  
//          frame = formatFram;  
//          System.out.println("FrameSize:" + "");  
  
            //a = producer.send(new ProducerRecord<String, byte[]>("ddp_video_source", Long  
            //      .toString(frameCount), frameArray));  
  
            a = producer.send(new ProducerRecord<String, byte[]>(topic, Long  
                    .toString(frameCount), frameArray));  
  
            System.out.println("Send one frame" + a.isDone());  
            try {  
                Thread.sleep(2000);  
            } catch (InterruptedException e) {  
                // TODO Auto-generated catch block  
                e.printStackTrace();  
            }  
  
        }  
  
        producer.close();  
    }  
  
    public static void main(String[] args) throws UnsupportedEncodingException {  
  
    	kafka_distribution distributor = new kafka_distribution();  
        //  distributor.Init(URLDecoder.decode(args[0], "UTF-8"), args[1], args[2]);  
//        distributor.Init(args[0], args[1], args[2]);  
        Thread producerProcess = new Thread(distributor);  
        producerProcess.start();  
  
    }  
  
} 