# KafkaClusteredTaskManager

A fault-tolerant scheduler that allows scheduling of periodic tasks to a single node in a clustered environment, with the help of Kafka messaging system.

KafkaNodeTaskScheduler is designed to provide exclusive execution on a single node by scheduling tasks to run on that node, ensuring that no other nodes execute the same task simultaneously. The scheduler is fault-tolerant, meaning that it can handle node failures and redistribute the failed node's tasks to other nodes in the cluster.

KafkaNodeTaskScheduler uses a single Kafka topic for messaging and coordination between the nodes. The topic serves as a centralized communication channel for the scheduler, allowing it to maintain a unified view of the state of the nodes and the tasks they are responsible for.

With KafkaNodeTaskScheduler, you can easily schedule periodic tasks to run on a single node in a clustered environment, with fault tolerance and minimal persistence requirements.
