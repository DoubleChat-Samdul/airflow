### 채팅방 일정 알림기능
![image](https://github.com/user-attachments/assets/a3bbdf76-c42e-4430-acff-76f20d5fd339)
이 기능은 Airflow를 사용해 매일 아침 9시 30분에 Kafka를 통해 채팅방에 미팅 5분 전 알림 메시지를 자동 전송합니다. PythonOperator로 KafkaProducer를 호출하여, 메시지를 Kafka 토픽으로 전송하는 방식으로 구현되었습니다.
