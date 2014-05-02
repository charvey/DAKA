TASK_NAME := ClassifyPeople

$(TASK_NAME).jar:
	mkdir bin -p
	javac -cp '../lib/*' -d bin ./*.java
	jar -cf $(TASK_NAME).jar -C bin .

deploy: $(TASK_NAME).jar
	cp $(TASK_NAME).jar ../bin/

clean:
	rm -rf bin $(TASK_NAME).jar
