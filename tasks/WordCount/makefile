TASK_NAME := WordCount

build:
	javac -classpath ../lib/*.jar -d bin ./*.java
	jar -cf $(TASK_NAME).jar -C bin .

deploy:
	cp $(TASK_NAME).jar ../../bin/tasks/

clean:
	rm -rf *.class *.jar
