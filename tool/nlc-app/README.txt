1. Each log should be put in one line
2. After run nlc, check content of ./logs/xxx.log.PROGRESS, 1 means Success, others means Failure
3. In order to run nlc repeatedly with existed logs, please remove ./logs/xxx.log.PROGRESS before run nlc
4. In order to produce message into your localhost machine, please change "hostname" of $KAFKA_HOME/config/server.properties to use your machine IP and restart broker, NOTICE that you can NOT use localhost any more. If need to use localhost please comment "hostname" config again.