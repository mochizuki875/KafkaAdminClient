package com.example.kafkaadminclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// kafkaライブラリインポート（JRE8を使用する必要あり）
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class kafkaAdminClient {

	public static void main(String[] args) throws IOException{

        // Kafka Broker指定
        String broker = "kafka.cluster.local:31090,kafka.cluster.local:31091,kafka.cluster.local:31092";
        // 作成トピック
        String topic1 = "topic-1";
        String topic2 = "topic-2";
        String topic3 = "topic-3";

    	// 標準入力から値取得
        BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
        // OP変数初期化
        int operation = 1;

        // Kafkaの設定情報を持つPropertiesインスタンスの作成
        Properties props = new Properties();
	    props.setProperty("bootstrap.servers", broker);
	    props.setProperty("enable.auto.commit", "true");
	    props.setProperty("auto.commit.interval.ms", "1000");
	    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 新規AdminClientインスタンスを作成
	    Admin client = AdminClient.create(props);

	    while(operation != 0 ) {
        	System.out.println("操作を選んでください");
        	System.out.println("[1] Get Topic List");
        	System.out.println("[2] Create Topics");
        	System.out.println("[3] Delete Topics");
        	System.out.println("[0] Exit");

        	operation = Integer.parseInt(br.readLine());

        	switch(operation) {
        		case 0:
        			System.out.println("Exit");
        			break;
        		case 1:
        			// ①トピック一覧取得
        			// listTopicsメソッドでトピック情報を取得
        			ListTopicsResult listTopicsResult = client.listTopics();

        			try {
        				// Topic一覧をSet<String>型で取得する
        				Set<String> topics = listTopicsResult.names().get();
        				// Set型のtopicsインスタンスに格納された値を全て出力
        				for(String name : topics)
        				{
        					System.out.println(name);
        				}
        				System.out.println();
        			} catch ( InterruptedException | ExecutionException e ) {
        				throw new IllegalStateException(e);
        			}
        			break;
        		case 2:
        			// ②トピック作成
        			// createTopicsメソッドで引数に指定したトピックを作成
        			CreateTopicsResult createTopicsResult = client.createTopics(Arrays.asList(
        					new NewTopic(topic1, 1, (short) 1),
        					new NewTopic(topic2, 1, (short) 1),
        					new NewTopic(topic3, 1, (short) 1)
        			));
        			// 作成結果の確認（ここを呼び出さないと作成が実行されない）
        			try {
        				createTopicsResult.all().get();
        			} catch ( InterruptedException | ExecutionException e ) {
//        				throw new IllegalStateException(e);
        				System.out.println("Topics already exists.");
        			}
        			break;
        		case 3:
        			// ③トピック削除
        			// deleteTopicsメソッドで引数に指定したトピックを削除
        			DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList(topic1, topic2, topic3));

        			// 削除結果の確認（ここを呼び出さないと削除が実行されない）
        			try {
        				deleteTopicsResult.all().get();
        				System.out.println("Topics " + " deleted.");
        				System.out.println("");
        			} catch ( InterruptedException | ExecutionException e ) {
//        				throw new IllegalStateException(e);
        				System.out.println("Topics don't exists.");
        			}
        			break;
        	}
	    }
    }
}
