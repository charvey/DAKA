TASK_NAME := CountAll

build:
	javac -cp '../lib/*' -d bin ./*.java
	jar -cf $(TASK_NAME).jar -C bin .

deploy:
	cp $(TASK_NAME).jar ../bin/

clean:
	rm -rf bin/* $(TASK_NAME).jar
