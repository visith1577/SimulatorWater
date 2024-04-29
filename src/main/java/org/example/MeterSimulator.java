package org.example;

import io.github.cdimascio.dotenv.Dotenv;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import java.util.Objects;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class MeterSimulator {

    public static final Dotenv dotenv = Dotenv
            .configure()
            .load();
    private static final String BROKER_URL = "ssl://" + dotenv.get("HIVE_URL") + ":" + dotenv.get("HIVE_PORT");
    private static final String TOPIC = dotenv.get("TOPIC");
    private static final String CONTROL_TOPIC = dotenv.get("CNTR_TOPIC");
    private static final int INTERVAL_MILLIS = 5000; // 5 seconds
    private MqttClient mqttClient;
    private final Random random;
    private int lastReading = 0;
    private int lastReadingBeforeDisconnect = 0;
    private boolean disconnectMode = false;

    public MeterSimulator() {
        try {
            assert TOPIC != null;
            mqttClient = new MqttClient(BROKER_URL, TOPIC, new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setUserName(dotenv.get("HIVE_USER"));
            options.setPassword(Objects.requireNonNull(dotenv.get("HIVE_PASS")).toCharArray());
            options.setSocketFactory(SSLSocketFactory.getDefault());
            mqttClient.setCallback(new MqttControlMessageCallback());
            mqttClient.connect(options);
            mqttClient.subscribe(CONTROL_TOPIC, 2);

            publishRetainedReading(String.valueOf(lastReading));

        } catch (MqttException e) {
            e.printStackTrace();
        }
        random = new Random();
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new PublishReadingTask(), 0, INTERVAL_MILLIS);
    }

    private int generateWaterMeterReading() {
        int minIncrement = 1;
        int maxIncrement = 3;
        int increment = minIncrement + random.nextInt((maxIncrement - minIncrement) + 1);
        int currentReading = lastReading + increment;
        lastReading = currentReading;
        return currentReading;
    }

    private void publishRetainedReading(String reading) {
        try {
            MqttMessage message = new MqttMessage(reading.getBytes());
            message.setRetained(true);
            message.setQos(2);
            mqttClient.publish(TOPIC, message.getPayload(), 2, true);
        } catch (MqttException e) {
            e.printStackTrace();
            disconnectMode = true;
            lastReadingBeforeDisconnect = lastReading;
        }
    }

    private class PublishReadingTask extends TimerTask {
        @Override
        public void run() {
            if (disconnectMode) {
                publishRetainedReading(String.valueOf(lastReadingBeforeDisconnect));
            } else {
                int simulatedReading = generateWaterMeterReading();
                publishRetainedReading(String.valueOf(simulatedReading));
            }
        }
    }

    private class MqttControlMessageCallback implements MqttCallback {
        @Override
        public void connectionLost(Throwable cause) {
            System.out.println("MQTT connection lost: " + cause.getMessage());
            while(!mqttClient.isConnected()) {
                try {
                    mqttClient.reconnect();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();

                }
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            System.out.println("Message received: " + new String(message.getPayload()));
            if (topic.equals(CONTROL_TOPIC)) {
                String payload = new String(message.getPayload());
                if (payload.equals("Connect")) {
                    disconnectMode = false;
                } else if (payload.equals("Disconnect")) {
                    System.out.println("Disconnecting from connection");
                    disconnectMode = true;
                    lastReadingBeforeDisconnect = lastReading;
                }
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            try {
                if(token.isComplete()) {
                    System.out.println("Message delivered successfully " + token.getMessageId());
                } else {
                    System.out.println("Message delivery failed " + token.getMessage());
                    token.getException().printStackTrace();
                }
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new MeterSimulator();
    }
}