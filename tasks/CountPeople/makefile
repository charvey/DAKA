HADOOP_HOME := /home/user/Desktop/hadoop-1.2.1

build:
	javac -classpath $(HADOOP_HOME)/hadoop-core-1.2.1.jar -d bin ./*.java
	jar -cf CountPeople.jar -C bin .

deploy:
	cp CountPeople.jar ../../bin/tasks/

clean:
	rm -rf *.class *.jar