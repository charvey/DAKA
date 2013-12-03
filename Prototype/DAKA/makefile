HADOOP_HOME := /home/user/Desktop/hadoop-1.2.1

daka: daka.class


daka.class: people.jar
	javac daka.java

people.jar:
	mkdir bin
	javac -classpath $(HADOOP_HOME)/hadoop-core-1.2.1.jar -d bin ../CSV/*.java
	jar -cf people.jar -C bin .

countall: daka
	java daka -t CountAll -i 500000.csv -o countall.txt

countpeople: daka
	java daka -t CountPeople -i 500000.csv -o countpeople.txt

up:
	$(HADOOP_HOME)/bin/start-all.sh

down:
	$(HADOOP_HOME)/bin/stop-all.sh

clean:
	rm -rf *.class *.jar
	rm -rf bin
	rm -rf input
	rm -rf output
