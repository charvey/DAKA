# DAKA Deployment

## Description

This directory is a representation of the deployed project.

## Compiling

Fully build and deploy daka and tasks.

	make all

Compile the DAKA framework by running the following command.

	make daka

Update the tasks' dependency on the DAKA framework.

	make update-dependency

Compile all tasks by running the following command.

	make tasks

Deploy the DAKA and tasks to this directory.

	make deploy

Completely clean this directory to ensure proper deploy.

	make clean

## Usage

### Deployment

The steps below describe how to properly deploy the software to this directory.

1. Run the 'make clean' command from the Compiling section.
1. Run the 'make all' command from the Compiling section.

At this point the software should be deployed.

### Interface

To run DAKA, invoke it with Java's jar interface. You must specify which task you wish to run using the -t flag. Each task will have additional arguments which must be passed.

	java -jar daka.jar -t TaskName

### Tasks

There are currently five tasks available.

For more information look at the [documentation](../tasks/readme.md) for each task.

* WordSet - Takes a body of words and generates a text for use with WordCount
* WordCount - Counts the occurences of each word in a body of text
* FPDriver - Analyzes a csv file for frequent patterns
* FPReader - Displays the results of the frequent pattern analysis
* ClassifyPeople - Trains or classifies a product classifier for iPipeline data
